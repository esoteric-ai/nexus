# app/routes/openai.py

import uuid
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.storage import save_task, get_or_create_job, workers_storage

# A simple HTTP client class to act as the recipient of completed tasks.
class HTTPClient:
    def __init__(self):
        # A future that will be set once a worker submits a result.
        self.future = asyncio.get_event_loop().create_future()

    async def return_tasks(self, tasks: list):
        if not self.future.done():
            self.future.set_result(tasks)
        return True

# -------------------------
# OpenAI-compatible models
# -------------------------

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    temperature: Optional[float] = 1.0
    # Add additional OpenAI fields if needed

class ChatCompletionChoice(BaseModel):
    index: int
    message: ChatMessage
    finish_reason: str

class ChatCompletionResponse(BaseModel):
    id: str
    object: str
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    usage: Dict[str, int]

# Create an APIRouter for the OpenAI endpoint.
router = APIRouter()

@router.post("/v1/chat/completions")
async def chat_completions(request_data: ChatCompletionRequest):
    """
    This endpoint mimics OpenAI's /v1/chat/completions.
    When called, it creates a new task (with the provided messages and model)
    and then waits for a worker to process the task and return a result.
    """
    # Generate a unique job id (prefix with "http_" to distinguish from bound jobs)
    job_id = "http_" + str(uuid.uuid4())
    http_client = HTTPClient()
    
    # Create a job and bind the HTTP client to it.
    job = get_or_create_job(job_id)
    job["client"] = http_client

    # Create a new task based on the incoming request.
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "job_name": job_id,
        "models": [request_data.model],  # the worker will pick tasks matching its active_model
        "messages": [msg.dict() for msg in request_data.messages],
        "temperature": request_data.temperature,
        # You can include any extra fields here if needed.
    }
    
    # Save the new task in our in-memory storage.
    save_task(task, job_id)

    # Notify every connected worker that a new task is available.
    for worker_info in workers_storage.values():
        worker = worker_info.get("worker")
        if worker:
            # Use create_task so notifications run concurrently.
            asyncio.create_task(worker.notify_tasks_available())

    # Wait (up to 60 seconds) for a worker to process the task.
    try:
        completed_tasks = await asyncio.wait_for(http_client.future, timeout=60.0)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for worker response")

    if not completed_tasks:
        raise HTTPException(status_code=500, detail="No completed tasks received")

    # Assume the first completed task contains the answer.
    completed_task = completed_tasks[0]
    result_content = completed_task.get("result")
    if result_content is None:
        raise HTTPException(status_code=500, detail="Completed task missing 'result' field")

    # Build an OpenAI-compatible response.
    response = {
        "id": f"chatcmpl-{uuid.uuid4()}",
        "object": "chat.completion",
        "created": int(datetime.utcnow().timestamp()),
        "model": request_data.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": result_content
                },
                "finish_reason": "stop"
            }
        ],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0
        }
    }
    return response
