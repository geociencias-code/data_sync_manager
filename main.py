import os
import sys
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from app.config import STATIC_DIR, BASE_DIR, get_lan_ip, get_tailscale_ip
from app.api.routes import router
from app.sync_engine import sync_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BASE_DIR / "data" / "app.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("sync_app.main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application Lifespan:
    1. Runs crash recovery and checks previous running state / auto_start.
    2. Starts the background scheduler loop.
    3. Handles clean shutdown.
    """
    logger.info("Iniciando aplicación SFTP -> S3/FTP Data Sync...")
    
    # Run recovery & check auto-start
    sync_engine.init_and_recover()
    from app.websocket_manager import ws_manager
    ws_manager.set_loop(asyncio.get_running_loop())
    
    # Start scheduler background task
    scheduler_task = asyncio.create_task(sync_engine.scheduler_loop())
    sync_engine.background_task = scheduler_task
    
    yield
    
    # Shutdown sequence
    logger.info("Deteniendo servicios y cerrando tareas en segundo plano...")
    sync_engine.stop()
    if scheduler_task:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
    logger.info("Aplicación cerrada correctamente.")

# Initialize FastAPI app
app = FastAPI(
    title="SFTP to S3/FTP Data Sync Manager",
    description="Gestor de sincronización de datos de SFTP hacia S3 o FTP con rclone, persistencia SQLite y contingencia ante reinicios.",
    version="1.0.0",
    lifespan=lifespan
)

# CORS Middleware (if accessing from another device on the network)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ensure static folder exists and mount it
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include API and UI routes
app.include_router(router)

if __name__ == "__main__":
    from app.config import get_all_network_interfaces
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    interfaces = get_all_network_interfaces()
    
    print(f"\n=======================================================")
    print(f"🚀 DataSync Pro: SFTP ➔ S3 / FTP Manager")
    print(f"📡 Servidor activo en puerto: {port}")
    print(f"🌐 Enlaces de acceso detectados:")
    for item in interfaces:
        print(f"   • {item['label']:<32} ➔ {item['url']}")
    print(f"=======================================================\n")
    uvicorn.run("main:app", host=host, port=port, reload=False)

