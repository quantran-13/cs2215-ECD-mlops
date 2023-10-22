from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.views import api_router 
from api.cronjob import trigger_api_router
# from api.config import get_settings


def get_app() -> FastAPI:
    """Create FastAPI app."""

    app = FastAPI(
        title="Energy Consumption API",
        docs_url=f"/api/v1/docs",
        redoc_url=f"/api/v1/redoc",
        openapi_url=f"/api/v1/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix=f"/api/v1", tags=["Application API"])
    app.include_router(trigger_api_router, prefix=f"/trigger/v1", tags=["Trigger API"])

    return app
