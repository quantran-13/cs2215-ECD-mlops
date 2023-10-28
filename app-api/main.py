from api.application import get_app

if __name__ == "__main__":
    app = get_app()

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        workers=1,
    )
