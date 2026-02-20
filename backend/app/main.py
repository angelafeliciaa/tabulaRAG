from fastapi import FastAPI

app = FastAPI(title="TabulaRAG API")


@app.get("/health")
def health():
    return {"status": "ok"}
