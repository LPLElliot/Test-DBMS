# -----------------------
# log_db.py
# author: Ruizhe Yang   419198812@qq.com
# -----------------------------------
# Log management module
# Handles transaction logging including before/after images and transaction states
# ---------------------------------------

import os
import threading
import datetime

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # ensure log directory exists

# log file path definitions
BEFORE_IMAGE_FILE = os.path.join(LOG_DIR, "before_image.log")
AFTER_IMAGE_FILE = os.path.join(LOG_DIR, "after_image.log")
ACTIVE_TX_FILE = os.path.join(LOG_DIR, "active_tx.log")
COMMIT_TX_FILE = os.path.join(LOG_DIR, "commit_tx.log")

# ----------------------------------------------
# Author: Ruizhe Yang   419198812@qq.com
# Log Manager class for handling all logging operations
# ----------------------------------------------
class LogManager:
    lock = threading.Lock()

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Write log message to specified file
    # Input:
    #       file_path: target log file path
    #       msg: message content to write
    # Output:
    #       None
    # ----------------------------------------------
    @staticmethod
    def _write_log(file_path, msg):
        with LogManager.lock:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Format data for logging (remove b'' formatting)
    # Input:
    #       data: data to format (can be bytes, string, list, etc.)
    # Output:
    #       str: formatted string without b'' prefixes
    # ----------------------------------------------
    @staticmethod
    def _format_data(data):
        if data is None:
            return "NULL"
        if isinstance(data, bytes):
            return data.decode('utf-8', 'ignore').strip()
        if isinstance(data, str):
            return data.strip()
        if isinstance(data, (list, tuple)):
            formatted_items = []
            for item in data:
                if isinstance(item, bytes):
                    formatted_items.append(item.decode('utf-8', 'ignore').strip())
                elif isinstance(item, str):
                    formatted_items.append(item.strip())
                else:
                    formatted_items.append(str(item))
            return f"[{', '.join(formatted_items)}]"
        return str(data)

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Log before image with operation name
    # Input:
    #       tx_id: transaction ID
    #       table: table name
    #       record: record data before operation
    #       operation: operation name (CREATE_TABLE, DROP_TABLE, INSERT, UPDATE, DELETE)
    # Output:
    #       None
    # ----------------------------------------------
    @staticmethod
    def log_before_image(tx_id, table, record, operation="UNKNOWN"):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table_name = LogManager._format_data(table)
        record_data = LogManager._format_data(record)
        
        log_msg = f"[{ts}] tx_id={tx_id} operation={operation} table='{table_name}' BEFORE={record_data}"
        LogManager._write_log(BEFORE_IMAGE_FILE, log_msg)

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Log after image with operation name
    # Input:
    #       tx_id: transaction ID
    #       table: table name
    #       record: record data after operation
    #       operation: operation name (CREATE_TABLE, DROP_TABLE, INSERT, UPDATE, DELETE)
    # Output:
    #       None
    # ----------------------------------------------
    @staticmethod
    def log_after_image(tx_id, table, record, operation="UNKNOWN"):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        table_name = LogManager._format_data(table)
        record_data = LogManager._format_data(record)
        
        log_msg = f"[{ts}] tx_id={tx_id} operation={operation} table='{table_name}' AFTER={record_data}"
        LogManager._write_log(AFTER_IMAGE_FILE, log_msg)

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Add active transaction with operation name
    # Input:
    #       tx_id: transaction ID
    #       operation: operation name
    # Output:
    #       None
    # ----------------------------------------------
    @staticmethod
    def add_active_tx(tx_id, operation="UNKNOWN"):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{ts}] tx_id={tx_id} operation={operation} status=ACTIVE"
        LogManager._write_log(ACTIVE_TX_FILE, log_msg)

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Add commit transaction with operation name
    # Input:
    #       tx_id: transaction ID
    #       operation: operation name
    # Output:
    #       None
    # ----------------------------------------------
    @staticmethod
    def add_commit_tx(tx_id, operation="UNKNOWN"):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{ts}] tx_id={tx_id} operation={operation} status=COMMIT"
        LogManager._write_log(COMMIT_TX_FILE, log_msg)

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Read log file contents
    # Input:
    #       file_path: log file path to read
    # Output:
    #       list: list of log entries
    # ----------------------------------------------
    @staticmethod
    def read_log_file(file_path):
        if not os.path.exists(file_path):
            return []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.readlines()
        except Exception as e:
            print(f"Error reading log file {file_path}: {e}")
            return []

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Clear all log files
    # Input:
    #       None
    # Output:
    #       None
    # ----------------------------------------------
    @staticmethod
    def clear_all_logs():
        log_files = [BEFORE_IMAGE_FILE, AFTER_IMAGE_FILE, ACTIVE_TX_FILE, COMMIT_TX_FILE]
        for log_file in log_files:
            try:
                if os.path.exists(log_file):
                    with open(log_file, "w", encoding="utf-8") as f:
                        f.truncate(0)
                    print(f"Cleared log file: {log_file}")
            except Exception as e:
                print(f"Error clearing log file {log_file}: {e}")

    # ----------------------------------------------
    # Author: Ruizhe Yang   419198812@qq.com
    # Get log statistics
    # Input:
    #       None
    # Output:
    #       dict: statistics of all log files
    # ----------------------------------------------
    @staticmethod
    def get_log_statistics():
        stats = {}
        log_files = {
            "before_image": BEFORE_IMAGE_FILE,
            "after_image": AFTER_IMAGE_FILE,
            "active_tx": ACTIVE_TX_FILE,
            "commit_tx": COMMIT_TX_FILE
        }
        for log_type, file_path in log_files.items():
            if os.path.exists(file_path):
                lines = LogManager.read_log_file(file_path)
                stats[log_type] = len(lines)
            else:
                stats[log_type] = 0
        return stats