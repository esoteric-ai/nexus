import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, AsyncGenerator

from fastapi import APIRouter, Request, HTTPException, BackgroundTasks, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from app.core.storage import workers_storage

from app.core.storage import (
    tasks_storage, 
    completed_tasks_storage, 
    completed_chunks_storage,
    get_or_create_job,
    save_task
)

router = APIRouter()

class OpenAIClient:
    """Client to handle communication with workers and receive completions"""
    def __init__(self):
        self.uid = str(uuid.uuid4())
        self.task_queue = asyncio.Queue()
        self.chunk_queues = {}
        self.job_name = f"openai_job_{self.uid}"
        
    async def return_tasks(self, tasks: List[Dict[str, Any]]) -> bool:
        """Callback used by workers to return completed tasks"""
        for task in tasks:
            await self.task_queue.put(task)
        return True
    
    async def notify_stream_update(self, task_id: str) -> None:
        """Callback used by workers to notify about streaming updates"""
        if task_id in completed_chunks_storage:
            chunks = completed_chunks_storage[task_id]
            if task_id not in self.chunk_queues:
                self.chunk_queues[task_id] = asyncio.Queue()
            
            for chunk in chunks:
                await self.chunk_queues[task_id].put(chunk)
            
            # Clear the processed chunks
            completed_chunks_storage[task_id] = []
    
    async def wait_for_completion(self, timeout=300):
        """Wait for a task to be completed"""
        try:
            return await asyncio.wait_for(self.task_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError("Timed out waiting for task completion")

async def stream_openai_response(client: OpenAIClient, task_id: str, request_id: str, model: str) -> AsyncGenerator[str, None]:
    """Generate streaming responses in OpenAI format"""
    # Initialize tracking variables
    chunk_queue = client.chunk_queues.get(task_id, asyncio.Queue())
    client.chunk_queues[task_id] = chunk_queue
    
    done = False
    
    while not done:
        try:
            # Wait a short time for a chunk
            chunk = await asyncio.wait_for(chunk_queue.get(), timeout=0.1)
            
            event_type = chunk.get("event")
            
            if event_type == "chunk":
                # Simply forward the chunk data from the worker
                chunk_data = chunk.get("data", {})
                if chunk_data:
                    if "model" in chunk_data:
                        chunk_data["model"] = "mist"
                    # Format properly for SSE with data: prefix
                    yield f"data: {json.dumps(chunk_data)}\n\n"
            
            elif event_type == "done":
                # Streaming is complete - send [DONE] marker
                yield "data: [DONE]\n\n"
                done = True
            
            elif event_type == "error":
                # Handle error case
                error_message = chunk.get("error", "Unknown error")
                system_fingerprint = f"fp_{uuid.uuid4().hex[:8]}"
                error_response = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": "mist",
                    "system_fingerprint": system_fingerprint,
                    "choices": [{
                        "index": 0,
                        "delta": {"content": f"\\nError: {error_message}"},
                        "logprobs": None,
                        "finish_reason": "error"
                    }]
                }
                yield f"data: {json.dumps(error_response)}\n\n"
                yield "data: [DONE]\n\n"
                done = True
                
        except asyncio.TimeoutError:
            # No new chunks, continue waiting
            pass
        
        # Check if the task is no longer in storage (might be completed or failed)
        if task_id not in tasks_storage and task_id not in completed_chunks_storage:
            # If we haven't seen a "done" event yet, this might be an error
            if not done:
                yield "data: [DONE]\n\n"
                done = True

def convert_messages_format(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert OpenAI message format to our internal format"""

    
    return messages

def format_openai_response(completion: Dict[str, Any], request_id: str, model: str) -> Dict[str, Any]:
    """Format a completion as an OpenAI API response"""
    text = completion.get("response", {}).get("text", "")
    tool_calls = completion.get("response", {}).get("tool_calls", None)
    
    # Create the message structure
    message = {"role": "assistant"}
    
    if tool_calls:
        message["content"] = None
        message["tool_calls"] = tool_calls
        finish_reason = "tool_calls"
    else:
        message["content"] = text
        finish_reason = "stop"
    
    # Estimate token counts (actual implementation would be more accurate)
    prompt_tokens = sum(len(str(m.get("content", ""))) // 4 for m in completion.get("conversation", []))
    completion_tokens = len(text) // 4
    
    return {
        "id": request_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "system_fingerprint": f"fp_{uuid.uuid4().hex[:8]}",
        "choices": [
            {
                "index": 0,
                "message": message,
                "logprobs": None,
                "finish_reason": finish_reason
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens
        }
    }

async def notify_all_workers_about_task():
    """Notify all connected workers that a new task is available"""
    for worker_info in workers_storage.values():
        worker = worker_info["worker"]
        await worker.notify_tasks_available()

@router.post("/v1/chat/completions")
async def chat_completions(request: Request, background_tasks: BackgroundTasks):
    """OpenAI-compatible chat completions endpoint"""
    try:
        # Parse request
        body = await request.json()
        
        # Extract parameters
        model = body.get("model", "default")
        messages = body.get("messages", [])
        stream = body.get("stream", False)
        max_tokens = body.get("max_tokens")
        temperature = body.get("temperature", 0.01)
        top_p = body.get("top_p", 0.98)
        tools = body.get("tools", [])
        tool_choice = body.get("tool_choice", "auto")
        mm_processor_kwargs = body.get("mm_processor_kwargs",{})
        
        # Generate unique request ID
        request_id = f"chatcmpl-{uuid.uuid4().hex[:10]}"
        
        # Convert messages to our internal format
        conversation = convert_messages_format(messages)
        
        # Create client
        client = OpenAIClient()
        
        # Register job
        job = get_or_create_job(client.job_name)
        job["client"] = client
        
        # Create task
        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "conversation": conversation,
            "models": [model],  # Model the task requires
            "stream": stream,
            "max_tokens": max_tokens,
            "params": {
                "temperature": temperature,
                "top_p": top_p
            },
            "tools": tools,
            "tool_choice": tool_choice,
            "mm_processor_kwargs": mm_processor_kwargs,
            "created_at": datetime.now().isoformat()
        }
        
        # Save task (which makes it available to workers)
        save_task(task, client.job_name)
        
        # Notify all workers about the new task
        await notify_all_workers_about_task()
        
        if stream:
            # Return streaming response
            return StreamingResponse(
                stream_openai_response(client, task_id, request_id, model),
                media_type="text/event-stream",
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                }
            )
        else:
            # Wait for completion and return
            try:
                completion = await client.wait_for_completion()
                
                return completion['response']
            except TimeoutError:
                raise HTTPException(status_code=408, detail="Request timeout")
                
    except Exception as e:
        print("Error processing request: ", str(e))
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")