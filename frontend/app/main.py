import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from frontend.app.routes import bars, pipeline

app = FastAPI(title="Alpaca Pipeline Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(bars.router, prefix="/api")
app.include_router(pipeline.router, prefix="/api/pipeline")


@app.get("/api/health")
def health():
    return {"status": "ok"}


_static = Path(__file__).parent / "static"
if _static.exists():
    app.mount("/assets", StaticFiles(directory=_static / "assets"), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa(full_path: str):
        return FileResponse(_static / "index.html")
