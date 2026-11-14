import logging
import sys

def get_logger(name: str) -> logging.Logger:
    # Logger dành riêng cho các job
    logger = logging.getLogger(name)
    if not logger.handlers:  # tránh gắn handler 2 lần nếu hàm được gọi lại
        handler = logging.StreamHandler(sys.stdout)
        # Định dạng log:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s", "%y/%m/%d %H:%M:%S"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        # tắt lan truyền log: log chỉ được handler bởi logger hiện tại
        # không để lan truyền lên logger tổ tiên
        logger.propagate = False
    return logger