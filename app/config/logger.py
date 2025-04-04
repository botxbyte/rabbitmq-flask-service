from loguru import logger
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class LoggerSetup:
    def __init__(self):
        self.base_log_dir = os.getenv("LOG_DIR", "app/static/logs/workers")
        self.daily_log_dir = self.get_daily_log_dir()

    def get_daily_log_dir(self):
        """Return the log directory for the current year/month/day."""
        now = datetime.now()
        daily_log_dir = os.path.join(self.base_log_dir, str(now.year), str(now.month).zfill(2), str(now.day).zfill(2))
        os.makedirs(daily_log_dir, exist_ok=True)
        return daily_log_dir

    def setup_logger(self):
        """Setup the main logger with console and file handlers."""
        # Remove default logger
        logger.remove()

        # Add console logger
        logger.add(sys.stdout, level="INFO")

        # Add file logger for all logs inside the daily folder
        logger.add(
            f"{self.daily_log_dir}/app.log",
            rotation="10 MB",
            retention="1 week",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | PID:{process} | {message}",
            level="INFO"
        )

        return logger

    def setup_worker_logger(self, pid=None):
        """Setup worker-specific logger."""
        # Remove default logger
        logger.remove()

        # Add console logger
        logger.add(sys.stdout, level="INFO")

        # Create log filename based on optional PID
        log_filename = "worker"
        if pid:
            log_filename += f"_{pid}"
        log_filename += ".log"

        # Configure logger
        logger.add(
            f"{self.daily_log_dir}/{log_filename}",
            rotation="10 MB",
            retention="1 week",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            level="INFO"
        )

        return logger

# Remove these lines as they cause issues
# logger_setup = LoggerSetup()
# app_logger = logger_setup.setup_logger()
# worker_logger = logger_setup.setup_worker_logger(pid=1234)