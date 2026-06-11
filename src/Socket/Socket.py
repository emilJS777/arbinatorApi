import logging
from json import dumps

from flask_sock import ConnectionClosed

from .ISocket import ISocket
from src import sock, app

with app.app_context():
    sids: list = []


class Socket(ISocket):

    @staticmethod
    @sock.route('/apphub')
    def connect(ws):
        logging.info("start ws")
        sids.append(ws)
        try:
            while True:
                message = ws.receive()
                if message is None:
                    break
        except ConnectionClosed as e:
            logging.info("error ws")
            logging.error(e)
        finally:
            if ws in sids:
                sids.remove(ws)
            logging.info("final ws")

    async def send(self, emit_name: str, data: dict) -> bool:
        payload = dumps({"topic": emit_name, "data": data}, default=str)
        stale_connections = []

        for sid in list(sids):
            try:
                sid.send(payload)
            except Exception as error:
                logging.error("Failed to send socket payload: %s", error)
                stale_connections.append(sid)

        for sid in stale_connections:
            if sid in sids:
                sids.remove(sid)

        return True
