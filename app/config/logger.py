from loguru import logger
import os
import sys
from datetime import datetime

# Define the base log directory
BASE_LOG_DIR = "app/static/logs/workers"

def get_daily_log_dir():
    """Return the log directory for the current year/month/day."""
    now = datetime.now()
    daily_log_dir = os.path.join(BASE_LOG_DIR, str(now.year), str(now.month).zfill(2), str(now.day).zfill(2))
    os.makedirs(daily_log_dir, exist_ok=True)
    return daily_log_dir

def setup_logger():
    """Setup the main logger with console and file handlers."""
    daily_log_dir = get_daily_log_dir()

    # Remove default logger
    logger.remove()

    # Add console logger
    logger.add(sys.stdout, level="INFO")

    # Add file logger for all logs inside the daily folder
    logger.add(
        f"{daily_log_dir}/app.log",
        rotation="10 MB",
        retention="1 week",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | PID:{process} | {message}",
        level="INFO"
    )

    return logger

def setup_worker_logger(pid=None):
    """Setup worker-specific logger.

    Args:
        pid (int, optional): Process ID of the worker.
    """
    daily_log_dir = get_daily_log_dir()

    # Remove default logger
    logger.remove()

    # Create log filename based on optional PID
    log_filename = "worker"
    if pid:
        log_filename += f"_{pid}"
    log_filename += ".log"

    # Configure logger
    logger.add(
        f"{daily_log_dir}/{log_filename}",
        rotation="10 MB",
        retention="1 week",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO"
    )

    return logger
