from api.clearml import clearml_router
from api.views import api_router
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


def get_app() -> FastAPI:
    """Create FastAPI app."""
    app = FastAPI(
        title="Energy Consumption API",
        docs_url="/api/docs",
        redoc_url="/api/v1/redoc",
        openapi_url="/api/v1/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api/v1", tags=["Application API"])
    app.include_router(clearml_router, prefix="/clearml", tags=["Trigger API"])

    return app
