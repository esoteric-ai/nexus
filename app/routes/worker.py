import uuid
from typing import List
from fastapi import APIRouter, WebSocket, HTTPException
from pydantic import BaseModel

from app.core.worker import Worker
from app.core.storage import workers_storage

router = APIRouter()

class RegisterRequest(BaseModel):
    name: str
    supported_models: List[str]
    active_model: str

@router.post("/register")
def register_worker(request: RegisterRequest):
    """
    Register a new worker with a given name, list of supported models,
    and the currently active model. Returns a unique worker_uid for the new worker.
    """
    # Validate that the active_model is in the list of supported_models

    if request.active_model not in request.supported_models:
        raise HTTPException(
            status_code=400,
            detail="active_model must be one of the supported_models"
        )

    worker_uid = str(uuid.uuid4())

    # Create the Worker instance
    new_worker = Worker(
        uid=worker_uid,
        name=request.name,
        supported_models=request.supported_models,
        active_model=request.active_model,
    )

    # Save to in-memory storage
    workers_storage[worker_uid] = {
        "uid": worker_uid,
        "name": request.name,
        "supported_models": request.supported_models,
        "active_model": request.active_model,
        "worker": new_worker
    }

    return {"worker_uid": worker_uid}

@router.websocket("/ws/{worker_uid}")
async def worker_websocket(websocket: WebSocket, worker_uid: str):
    """
    WebSocket endpoint for a registered worker.
    If a connection already exists for this worker, it is replaced.
    """
    await websocket.accept()

    # Retrieve the worker data from storage.
    worker_data = workers_storage.get(worker_uid)
    if not worker_data:
        # If no such worker exists, close the websocket.
        await websocket.close()
        return

    worker = worker_data["worker"]

    # Attach the new websocket connection.
    # (If a connection is already attached, it will be closed.)
    await worker.attach_websocket(websocket)

    # Let the Worker instance handle incoming messages indefinitely.
    await worker.run_forever()
