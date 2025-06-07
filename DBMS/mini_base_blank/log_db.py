import os
import threading
import datetime

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # 确保日志目录存在

# 后续日志文件路径定义
BEFORE_IMAGE_FILE = os.path.join(LOG_DIR, "before_image.log")
AFTER_IMAGE_FILE = os.path.join(LOG_DIR, "after_image.log")
ACTIVE_TX_FILE = os.path.join(LOG_DIR, "active_tx.log")
COMMIT_TX_FILE = os.path.join(LOG_DIR, "commit_tx.log")

class LogManager:
    lock = threading.Lock()

    @staticmethod
    def _write_log(file_path, msg):
        with LogManager.lock, open(file_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

    @staticmethod
    def log_before_image(tx_id, table, record):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LogManager._write_log(BEFORE_IMAGE_FILE, f"[{ts}] tx_id={tx_id} table={table} BEFORE={record}")

    @staticmethod
    def log_after_image(tx_id, table, record):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LogManager._write_log(AFTER_IMAGE_FILE, f"[{ts}] tx_id={tx_id} table={table} AFTER={record}")

    @staticmethod
    def add_active_tx(tx_id):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LogManager._write_log(ACTIVE_TX_FILE, f"[{ts}] tx_id={tx_id} ACTIVE")

    @staticmethod
    def add_commit_tx(tx_id):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LogManager._write_log(COMMIT_TX_FILE, f"[{ts}] tx_id={tx_id} COMMIT")

    @staticmethod
    def read_log_file(file_path):
        if not os.path.exists(file_path):
            return []
        with open(file_path, "r", encoding="utf-8") as f:
            return f.readlines()