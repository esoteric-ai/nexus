from datetime import datetime
import uuid
from typing import Any, Dict, List, Optional
from typing_extensions import TypedDict

from fastapi import APIRouter, WebSocket, HTTPException
from pydantic import BaseModel

from app.core.worker import Worker
from app.core.storage import workers_storage, historical_workers_storage, worker_models_status

router = APIRouter()

class ModelLoadConfig(TypedDict):
    num_gpu_layers: int = 0
    gpu_split: List[int] = [1]

class ModelPerformanceMetrics(TypedDict):
    parallel_requests: int = 1
    ram_requirement: int = 8000
    vram_requirement: List[int] = [8000]
    benchmark_results: Dict[str, Any] = {}

class ModelConfig(TypedDict):
    alias: str = None
    backend: str = None
    quant: str = None
    wrapper: str = None
    context_length: int = 8192
    
    api_name: str

    load_options: ModelLoadConfig
    performance_metrics: Optional[ModelPerformanceMetrics] = None

class GPUInfo(TypedDict):
    index: int
    name: str
    memory_total_mb: int

class RegisterRequest(BaseModel):
    name: str
    backend_types: List[str]
    supported_models: List[ModelConfig]
    gpu_info: Optional[List[Dict[str, Any]]] = None


@router.post("/register")
def register_worker(request: RegisterRequest):
    """
    Register a new worker with a given name, list of supported models,
    and supported backend types. Returns a unique worker_uid for the new worker.
    """
    print(request)

    worker_uid = str(uuid.uuid4())

    # Create the Worker instance with the new format
    new_worker = Worker(
        uid=worker_uid,
        name=request.name,
        supported_models=request.supported_models,
        backend_types=request.backend_types,
    )

    # Save to in-memory storage
    workers_storage[worker_uid] = {
        "uid": worker_uid,
        "name": request.name,
        "supported_models": request.supported_models,
        "backend_types": request.backend_types,
        "gpu_info": request.gpu_info,  # Store GPU info
        "worker": new_worker,
        "metrics_history": []  # Initialize metrics history
    }
    
    for uid, worker_data in list(historical_workers_storage.items()):
        if worker_data["name"] == request.name:
            # Remove the existing historical record with the same name
            del historical_workers_storage[uid]
            break
            
    historical_workers_storage[worker_uid] = {
        "uid": worker_uid,
        "name": request.name,
        "supported_models": request.supported_models,
        "backend_types": request.backend_types,
        "gpu_info": request.gpu_info,  # Store GPU info
        "first_seen": datetime.now().isoformat(),
        "last_seen": datetime.now().isoformat()
    }
    
    # Initialize model status tracking
    worker_models_status[worker_uid] = {
        "hot": [],
        "cold": []
    }
    
    print("registered worker", workers_storage[worker_uid])

    return {"worker_uid": worker_uid}

@router.post("/metrics/{worker_uid}")
async def worker_metrics(worker_uid: str, metrics: Dict[str, Any]):
    """
    Receive worker metrics data for monitoring purposes.
    """
    if worker_uid not in workers_storage:
        raise HTTPException(status_code=404, detail="Worker not found")
    
    # Add timestamp to metrics
    metrics["timestamp"] = datetime.now().isoformat()
    
    # Store metrics with a max history size
    max_history = 30  # Keep ~1 minutes of data (30 points at 2-second intervals)
    workers_storage[worker_uid]["metrics_history"].append(metrics)
    if len(workers_storage[worker_uid]["metrics_history"]) > max_history:
        workers_storage[worker_uid]["metrics_history"].pop(0)
    
    
    return {"status": "ok"}

@router.websocket("/ws/{worker_uid}")
async def worker_websocket(websocket: WebSocket, worker_uid: str):
    """
    WebSocket endpoint for a registered worker.
    If a connection already exists for this worker, it is replaced.
    """
    print("connect")
    await websocket.accept()
    print("connect 2")
    # Retrieve the worker data from storage.
    worker_data = workers_storage.get(worker_uid)
    if not worker_data:
        # If no such worker exists, close the websocket.
        await websocket.close()
        return

    # Update last_seen timestamp in historical records
    if worker_uid in historical_workers_storage:
        historical_workers_storage[worker_uid]["last_seen"] = datetime.now().isoformat()

    worker = worker_data["worker"]
    print("worker connected", worker_data)
    # Attach the new websocket connection.
    # (If a connection is already attached, it will be closed.)
    await worker.attach_websocket(websocket)

    # Let the Worker instance handle incoming messages indefinitely.
    await worker.run_forever()