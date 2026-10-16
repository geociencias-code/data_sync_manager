import asyncio
import json
import logging
from typing import Set, Dict, Any, Optional
from fastapi import WebSocket

logger = logging.getLogger("sync_app.websocket")

class WebSocketManager:
    """
    Manages active WebSocket client connections and broadcasts live push events
    (status updates, file transfer progress, task state changes, logs)
    eliminating the need for constant HTTP polling from the frontend.
    """
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.main_loop: Optional[asyncio.AbstractEventLoop] = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self.main_loop = loop

    def has_connections(self) -> bool:
        return len(self.active_connections) > 0

    @property
    def connection_count(self) -> int:
        return len(self.active_connections)

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        try:
            self.main_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        logger.info(f"Cliente WebSocket conectado. Conexiones activas: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"Cliente WebSocket desconectado. Conexiones activas: {len(self.active_connections)}")

    async def broadcast(self, event_type: str, data: Any = None):
        """
        Asynchronously broadcasts a JSON event to all connected clients.
        Automatically cleans up dead or closed sockets.
        """
        if not self.active_connections:
            return

        payload = json.dumps({
            "type": event_type,
            "data": data if data is not None else {}
        })

        dead_connections = set()
        for ws in list(self.active_connections):
            try:
                await ws.send_text(payload)
            except Exception as e:
                logger.debug(f"Error al enviar mensaje WebSocket a un cliente: {e}")
                dead_connections.add(ws)

        for ws in dead_connections:
            self.active_connections.discard(ws)

    def broadcast_sync(self, event_type: str, data: Any = None):
        """
        Safe non-blocking helper to trigger a broadcast from either synchronous
        or asynchronous contexts across threads.
        """
        if not self.active_connections:
            return

        # 1. Check if the current thread has an active event loop
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(self.broadcast(event_type, data))
                return
        except RuntimeError:
            pass

        # 2. Schedule on main application/test loop threadsafe
        if self.main_loop and self.main_loop.is_running():
            try:
                asyncio.run_coroutine_threadsafe(self.broadcast(event_type, data), self.main_loop)
            except Exception as e:
                logger.debug(f"Error scheduling coroutine threadsafe: {e}")

ws_manager = WebSocketManager()


