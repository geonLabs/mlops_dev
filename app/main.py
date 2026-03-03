from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.core.cors import setup_cors
from app.routers.cvat_webhook import router as cvat_webhook_router
from app.routers.upload import router as upload_router

app = FastAPI(title="Backend API")

setup_cors(app)
app.include_router(upload_router)
app.include_router(cvat_webhook_router)

FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def frontend_index():
    return FileResponse(FRONTEND_DIR / "index.html")
