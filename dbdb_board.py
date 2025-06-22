import sys
import json
import time
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
    QWidget, QPushButton, QTableWidget, QTableWidgetItem,
    QLabel, QLineEdit, QSpinBox, QTextEdit, QTabWidget,
    QMessageBox, QProgressBar, QGroupBox, QCheckBox,
    QStatusBar, QHeaderView
)
from PyQt6.QtCore import (
    QThread, QTimer, pyqtSignal, QObject, QMutex,
    QMutexLocker, Qt, QDateTime
)
from PyQt6.QtGui import QFont, QColor


class APIClient:
    """HTTP client for Flask API with connection pooling and retry logic"""

    def __init__(self, base_url="http://localhost:5000", timeout=30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'PyQt6-Desktop-App/1.0'
        })

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with error handling"""
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise APIException(f"API request failed: {str(e)}")

    def get_users(self, limit=100, offset=0) -> Dict[str, Any]:
        """Get users with pagination"""
        return self._make_request('GET', f'/users?limit={limit}&offset={offset}')

    def get_user(self, user_id: int) -> Dict[str, Any]:
        """Get specific user by ID"""
        return self._make_request('GET', f'/users/{user_id}')

    def create_user(self, name: str, email: str) -> Dict[str, Any]:
        """Create new user"""
        data = {"name": name, "email": email}
        return self._make_request('POST', '/users', json=data)

    def update_user(self, user_id: int, name: str = None, email: str = None) -> Dict[str, Any]:
        """Update user"""
        data = {}
        if name:
            data["name"] = name
        if email:
            data["email"] = email
        return self._make_request('PUT', f'/users/{user_id}', json=data)

    def delete_user(self, user_id: int) -> Dict[str, Any]:
        """Delete user"""
        return self._make_request('DELETE', f'/users/{user_id}')

    def execute_query(self, query: str, params: List = None) -> Dict[str, Any]:
        """Execute custom SQL query"""
        data = {"query": query, "allow_write": True}
        if params:
            data["params"] = params
        return self._make_request('POST', '/query', json=data)

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        return self._make_request('GET', '/stats')

    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        return self._make_request('GET', '/pool-stats')

    def health_check(self) -> Dict[str, Any]:
        """Check API health"""
        return self._make_request('GET', '/health')


class APIException(Exception):
    """Custom exception for API errors"""
    pass


class DataCache:
    """Simple cache for API responses with TTL"""

    def __init__(self, default_ttl=30):
        self.cache = {}
        self.default_ttl = default_ttl
        self.mutex = QMutex()

    def get(self, key: str) -> Optional[Any]:
        """Get cached data if not expired"""
        with QMutexLocker(self.mutex):
            if key in self.cache:
                data, expires_at = self.cache[key]
                if datetime.now() < expires_at:
                    return data
                else:
                    del self.cache[key]
            return None

    def set(self, key: str, data: Any, ttl: int = None) -> None:
        """Cache data with TTL"""
        if ttl is None:
            ttl = self.default_ttl

        expires_at = datetime.now() + timedelta(seconds=ttl)
        with QMutexLocker(self.mutex):
            self.cache[key] = (data, expires_at)

    def clear(self) -> None:
        """Clear all cached data"""
        with QMutexLocker(self.mutex):
            self.cache.clear()


class AutoFetchWorker(QObject):
    """Worker thread for auto-fetching data from API"""

    data_updated = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)
    status_changed = pyqtSignal(str)

    def __init__(self, api_client: APIClient, cache: DataCache):
        super().__init__()
        self.api_client = api_client
        self.cache = cache
        self.is_running = False
        self.fetch_interval = 15  # seconds
        self.last_fetch_time = None

        # Add jitter to prevent thundering herd
        self.jitter_range = 5  # ±5 seconds

    def start_fetching(self):
        """Start auto-fetch process"""
        self.is_running = True
        self.fetch_data()

    def stop_fetching(self):
        """Stop auto-fetch process"""
        self.is_running = False

    def set_interval(self, seconds: int):
        """Set fetch interval"""
        self.fetch_interval = max(5, seconds)  # Minimum 5 seconds

    def fetch_data(self):
        """Fetch data from API with caching and error handling"""
        if not self.is_running:
            return

        try:
            self.status_changed.emit("Fetching data...")

            # Check cache first
            cache_key = "users_data"
            cached_data = self.cache.get(cache_key)

            if cached_data:
                self.data_updated.emit(cached_data)
                self.status_changed.emit("Data loaded from cache")
            else:
                # Fetch from API
                start_time = time.time()
                result = self.api_client.get_users(limit=1000)
                fetch_time = time.time() - start_time

                # Cache the result
                self.cache.set(cache_key, result, ttl=30)

                # Emit data
                self.data_updated.emit(result)
                self.status_changed.emit(f"Data fetched in {fetch_time:.2f}s")

                self.last_fetch_time = datetime.now()

        except APIException as e:
            error_msg = f"API Error: {str(e)}"
            self.error_occurred.emit(error_msg)
            self.status_changed.emit("Error occurred")

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.error_occurred.emit(error_msg)
            self.status_changed.emit("Error occurred")

        # Schedule next fetch with jitter
        if self.is_running:
            jitter = random.randint(-self.jitter_range, self.jitter_range)
            next_interval = max(5, self.fetch_interval + jitter)
            QTimer.singleShot(next_interval * 1000, self.fetch_data)


class UserManagementWidget(QWidget):
    """Widget for user CRUD operations"""

    def __init__(self, api_client: APIClient):
        super().__init__()
        self.api_client = api_client
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        # Create user form
        form_group = QGroupBox("Create/Update User")
        form_layout = QVBoxLayout()

        # Form fields
        self.user_id_edit = QLineEdit()
        self.user_id_edit.setPlaceholderText("User ID (for update/delete)")

        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("Name")

        self.email_edit = QLineEdit()
        self.email_edit.setPlaceholderText("Email")

        # Buttons
        button_layout = QHBoxLayout()

        self.create_btn = QPushButton("Create User")
        self.create_btn.clicked.connect(self.create_user)

        self.update_btn = QPushButton("Update User")
        self.update_btn.clicked.connect(self.update_user)

        self.delete_btn = QPushButton("Delete User")
        self.delete_btn.clicked.connect(self.delete_user)

        button_layout.addWidget(self.create_btn)
        button_layout.addWidget(self.update_btn)
        button_layout.addWidget(self.delete_btn)

        form_layout.addWidget(QLabel("User ID:"))
        form_layout.addWidget(self.user_id_edit)
        form_layout.addWidget(QLabel("Name:"))
        form_layout.addWidget(self.name_edit)
        form_layout.addWidget(QLabel("Email:"))
        form_layout.addWidget(self.email_edit)
        form_layout.addLayout(button_layout)

        form_group.setLayout(form_layout)
        layout.addWidget(form_group)

        # Result display
        self.result_text = QTextEdit()
        self.result_text.setMaximumHeight(150)
        layout.addWidget(QLabel("Result:"))
        layout.addWidget(self.result_text)

        self.setLayout(layout)

    def create_user(self):
        """Create a new user"""
        name = self.name_edit.text().strip()
        email = self.email_edit.text().strip()

        if not name or not email:
            QMessageBox.warning(self, "Warning", "Please fill in both name and email")
            return

        try:
            result = self.api_client.create_user(name, email)
            self.result_text.setText(f"Success: {json.dumps(result, indent=2)}")
            self.name_edit.clear()
            self.email_edit.clear()
        except APIException as e:
            self.result_text.setText(f"Error: {str(e)}")

    def update_user(self):
        """Update an existing user"""
        user_id = self.user_id_edit.text().strip()
        name = self.name_edit.text().strip()
        email = self.email_edit.text().strip()

        if not user_id:
            QMessageBox.warning(self, "Warning", "Please enter User ID")
            return

        if not name and not email:
            QMessageBox.warning(self, "Warning", "Please enter at least name or email to update")
            return

        try:
            result = self.api_client.update_user(int(user_id), name or None, email or None)
            self.result_text.setText(f"Success: {json.dumps(result, indent=2)}")
        except (ValueError, APIException) as e:
            self.result_text.setText(f"Error: {str(e)}")

    def delete_user(self):
        """Delete a user"""
        user_id = self.user_id_edit.text().strip()

        if not user_id:
            QMessageBox.warning(self, "Warning", "Please enter User ID")
            return

        reply = QMessageBox.question(
            self, "Confirm Delete",
            f"Are you sure you want to delete user {user_id}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            try:
                result = self.api_client.delete_user(int(user_id))
                self.result_text.setText(f"Success: {json.dumps(result, indent=2)}")
                self.user_id_edit.clear()
            except (ValueError, APIException) as e:
                self.result_text.setText(f"Error: {str(e)}")


class MainWindow(QMainWindow):
    """Main application window"""

    def __init__(self):
        super().__init__()
        self.api_client = APIClient()
        self.cache = DataCache(default_ttl=30)
        self.auto_fetch_thread = None
        self.auto_fetch_worker = None

        self.init_ui()
        self.setup_auto_fetch()

        # Test API connection
        self.test_connection()

    def init_ui(self):
        self.setWindowTitle("Database API Client")
        self.setGeometry(100, 100, 1200, 800)

        # Central widget with tabs
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)

        # Control panel
        control_panel = self.create_control_panel()
        layout.addWidget(control_panel)

        # Tab widget
        tab_widget = QTabWidget()

        # Data table tab
        self.table_widget = QTableWidget()
        tab_widget.addTab(self.table_widget, "User Data")

        # User management tab
        self.user_mgmt_widget = UserManagementWidget(self.api_client)
        tab_widget.addTab(self.user_mgmt_widget, "User Management")

        # Statistics tab
        self.stats_widget = self.create_stats_widget()
        tab_widget.addTab(self.stats_widget, "Statistics")

        layout.addWidget(tab_widget)

        # Status bar
        self.statusBar().showMessage("Ready")

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.statusBar().addPermanentWidget(self.progress_bar)

    def create_control_panel(self):
        """Create control panel with auto-fetch settings"""
        group = QGroupBox("Auto-fetch Controls")
        layout = QHBoxLayout()

        # Auto-fetch toggle
        self.auto_fetch_checkbox = QCheckBox("Enable Auto-fetch")
        self.auto_fetch_checkbox.setChecked(True)
        self.auto_fetch_checkbox.toggled.connect(self.toggle_auto_fetch)

        # Interval setting
        layout.addWidget(QLabel("Interval (seconds):"))
        self.interval_spinbox = QSpinBox()
        self.interval_spinbox.setRange(5, 300)
        self.interval_spinbox.setValue(15)
        self.interval_spinbox.valueChanged.connect(self.update_fetch_interval)

        # Manual fetch button
        self.fetch_btn = QPushButton("Fetch Now")
        self.fetch_btn.clicked.connect(self.manual_fetch)

        # Clear cache button
        self.clear_cache_btn = QPushButton("Clear Cache")
        self.clear_cache_btn.clicked.connect(self.clear_cache)

        layout.addWidget(self.auto_fetch_checkbox)
        layout.addWidget(QLabel("Interval (seconds):"))
        layout.addWidget(self.interval_spinbox)
        layout.addWidget(self.fetch_btn)
        layout.addWidget(self.clear_cache_btn)
        layout.addStretch()

        group.setLayout(layout)
        return group

    def create_stats_widget(self):
        """Create statistics display widget"""
        widget = QWidget()
        layout = QVBoxLayout()

        # Stats display
        self.stats_text = QTextEdit()
        self.stats_text.setReadOnly(True)
        layout.addWidget(self.stats_text)

        # Refresh stats button
        refresh_btn = QPushButton("Refresh Statistics")
        refresh_btn.clicked.connect(self.refresh_stats)
        layout.addWidget(refresh_btn)

        widget.setLayout(layout)
        return widget

    def setup_auto_fetch(self):
        """Setup auto-fetch worker thread"""
        self.auto_fetch_thread = QThread()
        self.auto_fetch_worker = AutoFetchWorker(self.api_client, self.cache)
        self.auto_fetch_worker.moveToThread(self.auto_fetch_thread)

        # Connect signals
        self.auto_fetch_worker.data_updated.connect(self.update_table)
        self.auto_fetch_worker.error_occurred.connect(self.handle_error)
        self.auto_fetch_worker.status_changed.connect(self.statusBar().showMessage)

        self.auto_fetch_thread.start()

        # Start auto-fetch if enabled
        if self.auto_fetch_checkbox.isChecked():
            self.auto_fetch_worker.start_fetching()

    def toggle_auto_fetch(self, enabled):
        """Toggle auto-fetch on/off"""
        if enabled:
            self.auto_fetch_worker.start_fetching()
        else:
            self.auto_fetch_worker.stop_fetching()

    def update_fetch_interval(self, interval):
        """Update fetch interval"""
        if self.auto_fetch_worker:
            self.auto_fetch_worker.set_interval(interval)

    def manual_fetch(self):
        """Manually trigger data fetch"""
        if self.auto_fetch_worker:
            # Clear cache to force fresh fetch
            self.cache.clear()
            self.auto_fetch_worker.fetch_data()

    def clear_cache(self):
        """Clear data cache"""
        self.cache.clear()
        self.statusBar().showMessage("Cache cleared")

    def update_table(self, data):
        """Update table with fetched data"""
        if 'data' not in data:
            return

        users = data['data']

        # Setup table
        self.table_widget.setRowCount(len(users))
        if users:
            self.table_widget.setColumnCount(len(users[0]))
            self.table_widget.setHorizontalHeaderLabels(users[0].keys())

        # Populate data
        for row, user in enumerate(users):
            for col, (key, value) in enumerate(user.items()):
                item = QTableWidgetItem(str(value))
                self.table_widget.setItem(row, col, item)

        # Auto-resize columns
        self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)

        # Update status
        exec_time = data.get('execution_time', 0)
        self.statusBar().showMessage(f"Updated with {len(users)} users (exec: {exec_time:.3f}s)")

    def handle_error(self, error_msg):
        """Handle errors from auto-fetch worker"""
        self.statusBar().showMessage(f"Error: {error_msg}")
        QMessageBox.warning(self, "Error", error_msg)

    def test_connection(self):
        """Test API connection on startup"""
        try:
            result = self.api_client.health_check()
            self.statusBar().showMessage("Connected to API successfully")
        except APIException as e:
            QMessageBox.critical(
                self, "Connection Error",
                f"Failed to connect to API:\n{str(e)}\n\nPlease ensure the Flask API is running."
            )

    def refresh_stats(self):
        """Refresh statistics display"""
        try:
            db_stats = self.api_client.get_stats()
            pool_stats = self.api_client.get_pool_stats()

            stats_text = "=== Database Statistics ===\n"
            for key, value in db_stats.items():
                stats_text += f"{key}: {value}\n"

            stats_text += "\n=== Connection Pool Statistics ===\n"
            for key, value in pool_stats.items():
                if isinstance(value, float):
                    stats_text += f"{key}: {value:.2f}\n"
                else:
                    stats_text += f"{key}: {value}\n"

            stats_text += f"\n=== Cache Statistics ===\n"
            stats_text += f"cached_items: {len(self.cache.cache)}\n"

            self.stats_text.setText(stats_text)

        except APIException as e:
            QMessageBox.warning(self, "Error", f"Failed to get statistics: {str(e)}")

    def closeEvent(self, event):
        """Clean up on application close"""
        if self.auto_fetch_worker:
            self.auto_fetch_worker.stop_fetching()

        if self.auto_fetch_thread:
            self.auto_fetch_thread.quit()
            self.auto_fetch_thread.wait()

        event.accept()


def main():
    app = QApplication(sys.argv)

    # Set application style
    app.setStyle('Fusion')

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == '__main__':
    main()
