import sqlite3
import threading
import time
import json
from contextlib import contextmanager
from flask import Flask, request, jsonify
from functools import wraps
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLiteManager:
    """Thread-safe SQLite database manager with connection pooling"""
    
    def __init__(self, db_path, max_connections=10, timeout=30):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self._local = threading.local()
        self._lock = threading.RLock()
        self._connections = []
        self._available_connections = []
        
        # Initialize database and create sample table
        self._init_database()
    
    def _init_database(self):
        """Initialize database with proper settings"""
        with sqlite3.connect(self.db_path) as conn:
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=memory")
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB
            
            # Create sample table if it doesn't exist
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
    
    @contextmanager
    def get_connection(self):
        """Get a database connection with automatic cleanup"""
        conn = None
        try:
            conn = self._get_connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                self._return_connection(conn)
    
    def _get_connection(self):
        """Get a connection from the pool or create a new one"""
        with self._lock:
            if self._available_connections:
                return self._available_connections.pop()
            
            if len(self._connections) < self.max_connections:
                conn = sqlite3.connect(
                    self.db_path,
                    timeout=self.timeout,
                    check_same_thread=False
                )
                conn.row_factory = sqlite3.Row
                # Configure connection for concurrency
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=30000")  # 30 seconds
                
                self._connections.append(conn)
                return conn
            
            # Wait for available connection
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                if self._available_connections:
                    return self._available_connections.pop()
                time.sleep(0.1)
            
            raise Exception("Connection pool exhausted")
    
    def _return_connection(self, conn):
        """Return connection to the pool"""
        with self._lock:
            if len(self._available_connections) < self.max_connections:
                self._available_connections.append(conn)
            else:
                conn.close()
                if conn in self._connections:
                    self._connections.remove(conn)
    
    def execute_query(self, query, params=None, fetch=True):
        """Execute a query with retry logic"""
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    if fetch:
                        if query.strip().upper().startswith('SELECT'):
                            result = [dict(row) for row in cursor.fetchall()]
                            return {"data": result, "rowcount": len(result)}
                        else:
                            conn.commit()
                            return {"data": None, "rowcount": cursor.rowcount, "lastrowid": cursor.lastrowid}
                    else:
                        conn.commit()
                        return {"data": None, "rowcount": cursor.rowcount, "lastrowid": cursor.lastrowid}
                        
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"Database locked, retrying... (attempt {attempt + 1})")
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
                raise e
            except Exception as e:
                logger.error(f"Database error: {str(e)}")
                raise e
    
    def close_all_connections(self):
        """Close all connections in the pool"""
        with self._lock:
            for conn in self._connections + self._available_connections:
                try:
                    conn.close()
                except:
                    pass
            self._connections.clear()
            self._available_connections.clear()

# Initialize Flask app and database manager
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Initialize database manager
db_manager = SQLiteManager('app.db')

def handle_db_errors(f):
    """Decorator to handle database errors"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except sqlite3.IntegrityError as e:
            return jsonify({"error": "Data integrity error", "message": str(e)}), 400
        except sqlite3.OperationalError as e:
            return jsonify({"error": "Database operation error", "message": str(e)}), 500
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return jsonify({"error": "Internal server error", "message": str(e)}), 500
    
    return decorated_function

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": time.time()})

@app.route('/query', methods=['POST'])
@handle_db_errors
def execute_query():
    """Execute a custom SQL query"""
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({"error": "Query is required"}), 400
    
    query = data['query'].strip()
    params = data.get('params', [])
    
    # Basic SQL injection protection
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    if any(keyword in query.upper() for keyword in dangerous_keywords):
        if not data.get('allow_write', False):
            return jsonify({"error": "Write operations require allow_write=true"}), 403
    
    result = db_manager.execute_query(query, params)
    return jsonify(result)

# CRUD Operations for Users table (example)
@app.route('/users', methods=['GET'])
@handle_db_errors
def get_users():
    """Get all users with optional filtering"""
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    query = "SELECT * FROM users LIMIT ? OFFSET ?"
    result = db_manager.execute_query(query, [limit, offset])
    return jsonify(result)

@app.route('/users/<int:user_id>', methods=['GET'])
@handle_db_errors
def get_user(user_id):
    """Get a specific user by ID"""
    query = "SELECT * FROM users WHERE id = ?"
    result = db_manager.execute_query(query, [user_id])
    
    if not result['data']:
        return jsonify({"error": "User not found"}), 404
    
    return jsonify({"data": result['data'][0]})

@app.route('/users', methods=['POST'])
@handle_db_errors
def create_user():
    """Create a new user"""
    data = request.get_json()
    
    if not data or 'name' not in data or 'email' not in data:
        return jsonify({"error": "Name and email are required"}), 400
    
    query = "INSERT INTO users (name, email) VALUES (?, ?)"
    result = db_manager.execute_query(query, [data['name'], data['email']], fetch=False)
    
    return jsonify({
        "message": "User created successfully",
        "user_id": result['lastrowid']
    }), 201

@app.route('/users/<int:user_id>', methods=['PUT'])
@handle_db_errors
def update_user(user_id):
    """Update an existing user"""
    data = request.get_json()
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    # Build dynamic update query
    fields = []
    values = []
    
    if 'name' in data:
        fields.append('name = ?')
        values.append(data['name'])
    
    if 'email' in data:
        fields.append('email = ?')
        values.append(data['email'])
    
    if not fields:
        return jsonify({"error": "No valid fields to update"}), 400
    
    values.append(user_id)
    query = f"UPDATE users SET {', '.join(fields)} WHERE id = ?"
    
    result = db_manager.execute_query(query, values, fetch=False)
    
    if result['rowcount'] == 0:
        return jsonify({"error": "User not found"}), 404
    
    return jsonify({"message": "User updated successfully"})

@app.route('/users/<int:user_id>', methods=['DELETE'])
@handle_db_errors
def delete_user(user_id):
    """Delete a user"""
    query = "DELETE FROM users WHERE id = ?"
    result = db_manager.execute_query(query, [user_id], fetch=False)
    
    if result['rowcount'] == 0:
        return jsonify({"error": "User not found"}), 404
    
    return jsonify({"message": "User deleted successfully"})

@app.route('/users/bulk', methods=['POST'])
@handle_db_errors
def bulk_create_users():
    """Create multiple users in a single transaction"""
    data = request.get_json()
    
    if not data or 'users' not in data or not isinstance(data['users'], list):
        return jsonify({"error": "Users array is required"}), 400
    
    users = data['users']
    if not users:
        return jsonify({"error": "At least one user is required"}), 400
    
    # Validate all users first
    for user in users:
        if 'name' not in user or 'email' not in user:
            return jsonify({"error": "Each user must have name and email"}), 400
    
    # Bulk insert with transaction
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")
            
            user_ids = []
            for user in users:
                cursor.execute(
                    "INSERT INTO users (name, email) VALUES (?, ?)",
                    [user['name'], user['email']]
                )
                user_ids.append(cursor.lastrowid)
            
            conn.commit()
            
            return jsonify({
                "message": f"Created {len(user_ids)} users successfully",
                "user_ids": user_ids
            }), 201
            
        except Exception as e:
            conn.rollback()
            raise e

@app.route('/stats', methods=['GET'])
@handle_db_errors
def get_stats():
    """Get database statistics"""
    queries = [
        ("SELECT COUNT(*) as total_users FROM users", "total_users"),
        ("SELECT COUNT(*) as users_today FROM users WHERE DATE(created_at) = DATE('now')", "users_today"),
    ]
    
    stats = {}
    for query, key in queries:
        result = db_manager.execute_query(query)
        stats[key] = result['data'][0][key.replace('_', '')] if result['data'] else 0
    
    return jsonify(stats)

@app.teardown_appcontext
def close_db_connections(error):
    """Close database connections on app shutdown"""
    pass

if __name__ == '__main__':
    try:
        app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        db_manager.close_all_connections()
