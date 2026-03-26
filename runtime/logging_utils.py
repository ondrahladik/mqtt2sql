import logging
import sys


class ColoredFormatter(logging.Formatter):
    _RESET = "\033[0m"
    _COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[35m",
    }

    def __init__(self, use_color: bool) -> None:
        super().__init__("%(asctime)s %(levelname)s %(name)s %(message)s")
        self._use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        if self._use_color:
            color = self._COLORS.get(record.levelno, "")
            if color:
                record.levelname = f"{color}{record.levelname}{self._RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname


def configure_logging(log_level: str) -> logging.Logger:
    level = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Unsupported log level '{log_level}'")
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(ColoredFormatter(use_color=sys.stdout.isatty()))
    root_logger.addHandler(handler)
    logging.getLogger("werkzeug").setLevel(level)
    return logging.getLogger("mqtt2sql")
