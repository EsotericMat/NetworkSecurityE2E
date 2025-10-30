from loguru import logger
import sys

logger.add(sys.stdout, format="<level>{message}</level>")
logger.add('logs/logs')