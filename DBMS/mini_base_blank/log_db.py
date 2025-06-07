import os
import pickle
import threading

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
BLOCK_SIZE = 4096

BEFORE_IMAGE_FILE = os.path.join(LOG_DIR, "before_image.log")
AFTER_IMAGE_FILE = os.path.join(LOG_DIR, "after_image.log")
ACTIVE_TX_FILE = os.path.join(LOG_DIR, "active_tx.log")
COMMIT_TX_FILE = os.path.join(LOG_DIR, "commit_tx.log")

class LogManager:
    lock = threading.Lock()

    @staticmethod
    def write_block(file_path, block_data):
        with LogManager.lock, open(file_path, "ab") as f:
            data = pickle.dumps(block_data)
            if len(data) < BLOCK_SIZE:
                data += b'\x00' * (BLOCK_SIZE - len(data))
            f.write(data)

    @staticmethod
    def log_before_image(tx_id, table, record):
        LogManager.write_block(BEFORE_IMAGE_FILE, {
            "tx_id": tx_id,
            "table": table,
            "record": record
        })

    @staticmethod
    def log_after_image(tx_id, table, record):
        LogManager.write_block(AFTER_IMAGE_FILE, {
            "tx_id": tx_id,
            "table": table,
            "record": record
        })

    @staticmethod
    def add_active_tx(tx_id):
        LogManager.write_block(ACTIVE_TX_FILE, {"tx_id": tx_id})

    @staticmethod
    def add_commit_tx(tx_id):
        LogManager.write_block(COMMIT_TX_FILE, {"tx_id": tx_id})
    
    @staticmethod
    def read_log_file(file_path):
        logs = []
        if not os.path.exists(file_path):
            return logs
        with open(file_path, "rb") as f:
            while True:
                block = f.read(BLOCK_SIZE)
                if not block:
                    break
                try:
                    log = pickle.loads(block.rstrip(b'\x00'))
                    logs.append(log)
                except Exception:
                    continue
        return logs