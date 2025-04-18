# app/utils/logger.py
import logging
import os
from logging.handlers import RotatingFileHandler
import sys

class Logger:
    def __init__(self, name='iceberg-catalog', log_level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        self.formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        
        # Clear existing handlers to avoid duplicate logs
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)
        
        # Add file handler if LOG_FILE_PATH is set
        log_file_path = os.getenv('LOG_FILE_PATH')
        if log_file_path:
            # Create directory if it doesn't exist
            log_dir = os.path.dirname(log_file_path)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
                
            # Add rotating file handler (10MB per file, max 5 files)
            file_handler = RotatingFileHandler(
                log_file_path, 
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
            file_handler.setFormatter(self.formatter)
            self.logger.addHandler(file_handler)
    
    def get_logger(self):
        return self.logger

# Create a singleton logger instance
logger = Logger().get_logger()