from fastapi import APIRouter
from typing import List, Dict, Any

from app.core.storage import workers_storage, historical_workers_storage, worker_models_status

router = APIRouter()

@router.get("/get_workers")
def get_workers():
    """
    Get information about all workers, including:
    - Currently connected and historical workers
    - Worker details (name, supported models, etc.)
    - Hot models (currently loaded) and cold models (available for loading)
    - Latest metrics data (if available)
    
    Returns:
        Dict: {"workers": List of worker information dictionaries}
    
    Example:
        GET /info/get_workers
        
        Response:
        {
            "workers": [
                {
                    "uid": "abc-123",
                    "name": "Worker1",
                    "connected": true,
                    "supported_models": [...],
                    "backend_types": ["Instant", "Managed"],
                    "hot_models": [...],
                    "cold_models": [...],
                    "metrics": {...}
                },
                ...
            ]
        }
    """
    workers_info = []
    
    # Process all historical workers
    for worker_uid, worker_data in historical_workers_storage.items():
        # Check if the worker is currently connected and not marked as disconnected
        is_connected = worker_uid in workers_storage
        
        if is_connected:
            if 'worker' in workers_storage[worker_uid]:
                is_connected =  not workers_storage[worker_uid]['worker'].disconnected
                metrics = workers_storage[worker_uid].get("metrics_history", [])
        # Get model status information (hot/cold)
        model_status = worker_models_status.get(worker_uid, {"hot": [], "cold": []})
        
        
        worker_info = {
            "uid": worker_uid,
            "name": worker_data.get("name", "Unknown"),
            "connected": is_connected,
            "supported_models": worker_data.get("supported_models", []),
            "backend_types": worker_data.get("backend_types", []),
            "hot_models": model_status.get("hot", []),
            "cold_models": model_status.get("cold", []),
            "first_seen": worker_data.get("first_seen"),
            "last_seen": worker_data.get("last_seen"),
            "metrics": metrics
        }
        
        workers_info.append(worker_info)
    
    return {"workers": workers_info}