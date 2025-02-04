from fastapi import FastAPI
from app.routes.client import router as client_router
from app.routes.worker import router as worker_router

app = FastAPI()

# Include routers
app.include_router(client_router, prefix="/client")
app.include_router(worker_router, prefix="/worker")

@app.on_event("startup")
async def startup_event():
    pass

@app.on_event("shutdown")
async def shutdown_event():
    pass