import logging
import traceback
from core.storage.bot_database import BotDatabase


class DBLoggingHandler(logging.Handler):
    """
    Custom logging handler that writes logs to the SQLite database.
    """

    def __init__(self, bot_id: int = None, db: BotDatabase = None):
        super().__init__()
        self.bot_id = bot_id
        self.db = db or BotDatabase()

    def emit(self, record):
        try:
            # Skip if bot_id is not set (unless we want system-wide logs with bot_id=0 or NULL)
            # For now, we allow bot_id=None (Global Logs)
            bot_id = getattr(record, "bot_id", self.bot_id)

            # Map Python Log Level to Severity String
            severity = record.levelname  # INFO, ERROR, WARNING

            message = self.format(record)

            # Extract fix_action if present in extra
            # Usage: logger.error("Msg", extra={"fix_action": "Restart"})
            fix_action = getattr(record, "fix_action", None)

            # If exception info is present, append to message
            if record.exc_info:
                message += "\n" + "".join(traceback.format_exception(*record.exc_info))

            self.db.log_event(bot_id=bot_id, severity=severity, message=message, fix_action=fix_action)
        except Exception:
            self.handleError(record)
