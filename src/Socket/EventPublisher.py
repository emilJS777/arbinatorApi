import asyncio
import logging

from src.Socket.Socket import Socket


logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, socket=None):
        self.socket = socket or Socket()

    def publish(self, topic: str, data: dict) -> None:
        try:
            asyncio.run(self.socket.send(topic, data))
        except RuntimeError:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.socket.send(topic, data))
            except RuntimeError:
                logger.exception("Failed to publish websocket event %s", topic)
        except Exception:
            logger.exception("Failed to publish websocket event %s", topic)
