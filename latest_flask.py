import sqlite3
import threading
import time
import json
import re
from contextlib import contextmanager
from flask import Flask, request, jsonify
from functools import wraps
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLiteManager:
    """Thread-safe SQLite database manager with connection pooling"""
    
    def __init__(self, db_path, max_connections=25, timeout=60, max_retries=5):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self.max_retries = max_retries
        self._local = threading.local()
        self._lock = threading.RLock()
        self._connections = []
        self._available_connections = []
        self._connection_count = 0
        self._active_connections = 0
        
        # Performance monitoring
        self._request_count = 0
        self._error_count = 0
        self._start_time = time.time()
        
        # Initialize database and create sample table
        self._init_database()
    
    def _init_database(self):
        """Initialize database with proper settings for high concurrency"""
        with sqlite3.connect(self.db_path) as conn:
            # Optimized settings for high concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=memory")
            conn.execute("PRAGMA mmap_size=536870912")  # 512MB
            conn.execute("PRAGMA cache_size=10000")  # 10MB cache
            conn.execute("PRAGMA wal_autocheckpoint=1000")
            conn.execute("PRAGMA busy_timeout=60000")  # 60 seconds
            
            # Create sample table if it doesn't exist
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            conn.execute('CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at)')
            
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
                conn = self._available_connections.pop()
                self._active_connections += 1
                return conn
            
            if self._connection_count < self.max_connections:
                conn = sqlite3.connect(
                    self.db_path,
                    timeout=self.timeout,
                    check_same_thread=False
                )
                conn.row_factory = sqlite3.Row
                
                # Optimized connection settings for high concurrency
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=60000")  # 60 seconds
                conn.execute("PRAGMA cache_size=2000")  # 2MB per connection
                conn.execute("PRAGMA temp_store=memory")
                
                self._connections.append(conn)
                self._connection_count += 1
                self._active_connections += 1
                return conn
            
            # Wait for available connection with better timeout handling
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                if self._available_connections:
                    conn = self._available_connections.pop()
                    self._active_connections += 1
                    return conn
                time.sleep(0.05)  # Shorter sleep for better responsiveness
            
            raise Exception(f"Connection pool exhausted. Active: {self._active_connections}, Max: {self.max_connections}")
    
    def _return_connection(self, conn):
        """Return connection to the pool"""
        with self._lock:
            self._active_connections -= 1
            if len(self._available_connections) < self.max_connections:
                self._available_connections.append(conn)
            else:
                conn.close()
                if conn in self._connections:
                    self._connections.remove(conn)
                    self._connection_count -= 1
    
    def execute_select_query(self, query, params=None):
        """Execute SELECT query with retry logic and performance monitoring"""
        return self._execute_query_with_retry(query, params, operation_type="SELECT")
    
    def execute_insert_query(self, query, params=None):
        """Execute INSERT query with retry logic and performance monitoring"""
        return self._execute_query_with_retry(query, params, operation_type="INSERT")
    
    def execute_update_query(self, query, params=None):
        """Execute UPDATE/DELETE query with retry logic and performance monitoring"""
        return self._execute_query_with_retry(query, params, operation_type="UPDATE")
    
    def _execute_query_with_retry(self, query, params=None, operation_type="SELECT"):
        """Execute a query with retry logic and performance monitoring"""
        retry_delay = 0.05
        
        for attempt in range(self.max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    start_time = time.time()
                    
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    execution_time = time.time() - start_time
                    
                    # Log slow queries (>1 second)
                    if execution_time > 1.0:
                        logger.warning(f"Slow query detected: {execution_time:.2f}s - {query[:100]}...")
                    
                    self._request_count += 1
                    
                    if operation_type == "SELECT":
                        result = [dict(row) for row in cursor.fetchall()]
                        return {
                            "data": result,
                            "rowcount": len(result),
                            "execution_time": execution_time,
                            "operation": "SELECT"
                        }
                    else:
                        conn.commit()
                        return {
                            "data": None,
                            "rowcount": cursor.rowcount,
                            "lastrowid": cursor.lastrowid,
                            "execution_time": execution_time,
                            "operation": operation_type
                        }
                        
            except sqlite3.OperationalError as e:
                error_msg = str(e).lower()
                if ("database is locked" in error_msg or "busy" in error_msg) and attempt < self.max_retries - 1:
                    logger.warning(f"Database busy, retrying... (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
                self._error_count += 1
                raise e
            except Exception as e:
                self._error_count += 1
                logger.error(f"Database error: {str(e)}")
                raise e
    
    def get_pool_stats(self):
        """Get connection pool statistics"""
        with self._lock:
            uptime = time.time() - self._start_time
            return {
                "total_connections": self._connection_count,
                "active_connections": self._active_connections,
                "available_connections": len(self._available_connections),
                "max_connections": self.max_connections,
                "total_requests": self._request_count,
                "total_errors": self._error_count,
                "error_rate": self._error_count / max(self._request_count, 1) * 100,
                "requests_per_second": self._request_count / max(uptime, 1),
                "uptime_seconds": uptime
            }

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
            self._connection_count = 0
            self._active_connections = 0

# Initialize Flask app and database manager
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Initialize database manager with optimized settings for your load
db_manager = SQLiteManager('app.db', max_connections=25, timeout=60, max_retries=5)

def validate_query_type(query, expected_type):
    """Validate that the query matches the expected operation type"""
    query_upper = query.strip().upper()
    
    if expected_type == "SELECT":
        if not query_upper.startswith('SELECT'):
            return False, "Only SELECT queries are allowed for this endpoint"
        # Check for dangerous operations in SELECT
        dangerous_in_select = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        if any(keyword in query_upper for keyword in dangerous_in_select):
            return False, "SELECT queries cannot contain write operations"
    
    elif expected_type == "INSERT":
        if not query_upper.startswith('INSERT'):
            return False, "Only INSERT queries are allowed for this endpoint"
    
    elif expected_type == "UPDATE":
        if not (query_upper.startswith('UPDATE') or query_upper.startswith('DELETE')):
            return False, "Only UPDATE and DELETE queries are allowed for this endpoint"
        # Additional protection against dangerous operations
        if any(keyword in query_upper for keyword in ['DROP', 'TRUNCATE', 'ALTER', 'CREATE']):
            return False, "Dangerous operations are not allowed"
    
    return True, "Valid query"

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

@app.route('/select', methods=['POST'])
@handle_db_errors
def execute_select():
    """Execute SELECT queries only"""
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({"error": "Query is required"}), 400
    
    query = data['query'].strip()
    params = data.get('params', [])
    
    # Validate query type
    is_valid, error_msg = validate_query_type(query, "SELECT")
    if not is_valid:
        return jsonify({"error": error_msg}), 400
    
    try:
        result = db_manager.execute_select_query(query, params)
        return jsonify(result)
    except Exception as e:
        logger.error(f"SELECT query error: {str(e)}")
        return jsonify({"error": "Query execution failed", "message": str(e)}), 500

@app.route('/insert', methods=['POST'])
@handle_db_errors
def execute_insert():
    """Execute INSERT queries only"""
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({"error": "Query is required"}), 400
    
    query = data['query'].strip()
    params = data.get('params', [])
    
    # Validate query type
    is_valid, error_msg = validate_query_type(query, "INSERT")
    if not is_valid:
        return jsonify({"error": error_msg}), 400
    
    try:
        result = db_manager.execute_insert_query(query, params)
        return jsonify(result)
    except Exception as e:
        logger.error(f"INSERT query error: {str(e)}")
        return jsonify({"error": "Query execution failed", "message": str(e)}), 500

@app.route('/update', methods=['POST'])
@handle_db_errors
def execute_update():
    """Execute UPDATE and DELETE queries only"""
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({"error": "Query is required"}), 400
    
    query = data['query'].strip()
    params = data.get('params', [])
    
    # Validate query type
    is_valid, error_msg = validate_query_type(query, "UPDATE")
    if not is_valid:
        return jsonify({"error": error_msg}), 400
    
    try:
        result = db_manager.execute_update_query(query, params)
        return jsonify(result)
    except Exception as e:
        logger.error(f"UPDATE/DELETE query error: {str(e)}")
        return jsonify({"error": "Query execution failed", "message": str(e)}), 500

# Additional utility endpoints
@app.route('/pool-stats', methods=['GET'])
def get_pool_stats():
    """Get connection pool statistics and performance metrics"""
    return jsonify(db_manager.get_pool_stats())

@app.route('/stats', methods=['GET'])
@handle_db_errors
def get_stats():
    """Get database statistics using the select endpoint internally"""
    try:
        # Get total users
        total_users_result = db_manager.execute_select_query("SELECT COUNT(*) as total_users FROM users")
        total_users = total_users_result['data'][0]['total_users'] if total_users_result['data'] else 0
        
        # Get users created today
        users_today_result = db_manager.execute_select_query(
            "SELECT COUNT(*) as users_today FROM users WHERE DATE(created_at) = DATE('now')"
        )
        users_today = users_today_result['data'][0]['users_today'] if users_today_result['data'] else 0
        
        return jsonify({
            "total_users": total_users,
            "users_today": users_today
        })
    except Exception as e:
        logger.error(f"Stats query error: {str(e)}")
        return jsonify({"error": "Failed to get statistics", "message": str(e)}), 500

# Batch operations endpoints
@app.route('/batch-insert', methods=['POST'])
@handle_db_errors
def batch_insert():
    """Execute multiple INSERT queries in a single transaction"""
    data = request.get_json()
    
    if not data or 'queries' not in data or not isinstance(data['queries'], list):
        return jsonify({"error": "Queries array is required"}), 400
    
    queries = data['queries']
    if not queries:
        return jsonify({"error": "At least one query is required"}), 400
    
    # Validate all queries first
    for i, query_data in enumerate(queries):
        if 'query' not in query_data:
            return jsonify({"error": f"Query is required for item {i}"}), 400
        
        is_valid, error_msg = validate_query_type(query_data['query'], "INSERT")
        if not is_valid:
            return jsonify({"error": f"Invalid query at index {i}: {error_msg}"}), 400
    
    # Execute all queries in a single transaction
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")
            
            results = []
            start_time = time.time()
            
            for query_data in queries:
                query = query_data['query'].strip()
                params = query_data.get('params', [])
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                results.append({
                    "rowcount": cursor.rowcount,
                    "lastrowid": cursor.lastrowid
                })
            
            conn.commit()
            execution_time = time.time() - start_time
            
            return jsonify({
                "message": f"Executed {len(queries)} INSERT queries successfully",
                "results": results,
                "execution_time": execution_time,
                "operation": "BATCH_INSERT"
            })
            
    except Exception as e:
        logger.error(f"Batch INSERT error: {str(e)}")
        return jsonify({"error": "Batch insert failed", "message": str(e)}), 500

@app.route('/batch-update', methods=['POST'])
@handle_db_errors
def batch_update():
    """Execute multiple UPDATE/DELETE queries in a single transaction"""
    data = request.get_json()
    
    if not data or 'queries' not in data or not isinstance(data['queries'], list):
        return jsonify({"error": "Queries array is required"}), 400
    
    queries = data['queries']
    if not queries:
        return jsonify({"error": "At least one query is required"}), 400
    
    # Validate all queries first
    for i, query_data in enumerate(queries):
        if 'query' not in query_data:
            return jsonify({"error": f"Query is required for item {i}"}), 400
        
        is_valid, error_msg = validate_query_type(query_data['query'], "UPDATE")
        if not is_valid:
            return jsonify({"error": f"Invalid query at index {i}: {error_msg}"}), 400
    
    # Execute all queries in a single transaction
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")
            
            results = []
            start_time = time.time()
            
            for query_data in queries:
                query = query_data['query'].strip()
                params = query_data.get('params', [])
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                results.append({
                    "rowcount": cursor.rowcount
                })
            
            conn.commit()
            execution_time = time.time() - start_time
            
            return jsonify({
                "message": f"Executed {len(queries)} UPDATE/DELETE queries successfully",
                "results": results,
                "execution_time": execution_time,
                "operation": "BATCH_UPDATE"
            })
            
    except Exception as e:
        logger.error(f"Batch UPDATE error: {str(e)}")
        return jsonify({"error": "Batch update failed", "message": str(e)}), 500

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
