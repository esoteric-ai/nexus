# app/main.py

from fastapi import FastAPI
from app.routes.client import router as client_router
from app.routes.worker import router as worker_router
from app.routes.openai import router as openai_router  # <-- new import

app = FastAPI()

# Include routers
app.include_router(client_router, prefix="/client")
app.include_router(worker_router, prefix="/worker")
app.include_router(openai_router)  # This will add the /v1/chat/completions endpoint

@app.on_event("startup")
async def startup_event():
    pass

@app.on_event("shutdown")
async def shutdown_event():
    pass
