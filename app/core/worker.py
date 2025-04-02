import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from fastapi import WebSocket, WebSocketDisconnect
from app.core.storage import tasks_storage, completed_tasks_storage, completed_chunks_storage, get_or_create_job, workers_storage, worker_models_status

TIMEOUT = timedelta(minutes=5)
COMPLETED_TASKS_THRESHOLD = 10  # (Not used now as we flush immediately.)

class Worker:
    """
    Represents a connected worker.
    """
    def __init__(self, uid: str, name: str, supported_models: List[Dict], backend_types: List[str]):
        self.uid: str = uid
        self.name: str = name
        self.supported_models: List[Dict] = supported_models
        self.backend_types: List[str] = backend_types
        self.websocket: Optional[WebSocket] = None
        self.disconnected = False
        # A unique id representing the currently attached websocket.
        self.current_connection_id: Optional[str] = None
        
        # Track the models this worker can use
        self.available_models = [model.get("api_name") for model in supported_models if "api_name" in model]

    async def attach_websocket(self, websocket: WebSocket):
        """
        Attach a new websocket for this worker.
        If an existing connection is already attached, close it immediately.
        A new unique connection id is generated so that the run loop of a stale connection
        can detect it and exit.
        """
        if self.websocket is not None:
            try:
                await self.websocket.close(code=1000, reason="New connection established")
                print(f"[Worker] Closed stale websocket for worker '{self.name}'")
            except Exception as e:
                print(f"[Worker] Error closing stale websocket for worker '{self.name}': {e}")
        self.websocket = websocket
        self.current_connection_id = str(uuid.uuid4())
        print(f"[Worker] New websocket attached for worker '{self.name}' (connection_id={self.current_connection_id})")

    async def run_forever(self):
        """
        Read messages continuously until disconnect.
        The loop "remembers" the connection id that was current when the loop started.
        If a new connection is attached (and thus the connection id changes),
        the loop exits gracefully.
        """
        if not self.websocket:
            raise ConnectionError("No WebSocket attached to worker.")

        # Remember the connection id for this run loop.
        local_connection_id = self.current_connection_id

        try:
            while True:
                data = await self.websocket.receive_json()
                await self.handle_incoming_message(data)

                # Check if a new connection has been attached.
                if self.current_connection_id != local_connection_id:
                    print(f"[Worker] New connection detected for worker '{self.name}', ending run loop for stale connection.")
                    break
        except WebSocketDisconnect:
            self.release_assigned_tasks()
            self.websocket = None
            self.current_connection_id = None
            self.disconnected = True
            print(f"Worker '{self.name}' (uid={self.uid}) disconnected.")
        except Exception as e:
            print(f"Worker '{self.name}' encountered error: {e}")

    async def handle_incoming_message(self, data: dict):
        action = data.get("action")
        request_id = data.get("request_id")
        if action == "request_tasks":
            number = data.get("number", 1)
            hot_models = data.get("hot_models", [])
            cold_models = data.get("cold_models", [])

            # Save the hot and cold models status
            if self.uid in worker_models_status:
                worker_models_status[self.uid]["hot"] = hot_models
                worker_models_status[self.uid]["cold"] = cold_models

            tasks = self._get_available_tasks(number, hot_models, cold_models)
            await self.websocket.send_json({
                "action": "response_tasks",
                "request_id": request_id,
                "tasks": tasks,
            })
        elif action == "submit_completed_tasks":
            tasks = data.get("tasks", [])
            await self._complete_tasks(tasks)
            await self.websocket.send_json({
                "action": "ack_completed",
                "request_id": request_id,
                "ack": True,
            })
        elif action == "stream_event":
            # Handle streaming events
            task_id = data.get("task_id")
            event = data.get("event", {})
            await self._handle_stream_event(task_id, event)
            if event.get("event") != "chunk":
                await self.websocket.send_json({
                    "action": "ack_stream_event",
                    "request_id": request_id,
                    "ack": True,
                })
        else:
            print(f"Unhandled message from worker '{self.name}':", data)

    def _get_available_tasks(self, number: int, hot_models: List[str], cold_models: List[str]) -> List[Dict[str, Any]]:
        now = datetime.now()
        available_tasks = []
        
        
        # First try to find tasks for hot models
        if hot_models:
            for task in list(tasks_storage.values()):
                task_models = task.get("models", [])
                
                hot_model_aliases = [model.get('alias') for model in hot_models]
                if not any(model in hot_model_aliases for model in task_models):
                    continue
                
                
                last_retrieval = task.get("last_retrieval")
                if last_retrieval:
                    if isinstance(last_retrieval, str):
                        last_retrieval = datetime.fromisoformat(last_retrieval)
                    if (now - last_retrieval) < TIMEOUT:
                        continue
                    
                
                task["last_retrieval"] = now.isoformat()
                task["current_worker_uid"] = self.uid
                available_tasks.append(task)
                if len(available_tasks) >= number:
                    return available_tasks
        
        # If we didn't fill the quota with hot models, try cold models
        if len(available_tasks) < number and cold_models:
            
            for task in list(tasks_storage.values()):
                # Skip tasks already assigned
                if task.get("id") in [t.get("id") for t in available_tasks]:
                    continue
                
                
                task_models = task.get("models", [])
                
                # Check if any of task's models are in cold_models
                cold_model_aliases = [model.get('alias') for model in cold_models]
                if not any(model in cold_model_aliases for model in task_models):
                    continue
                    
                last_retrieval = task.get("last_retrieval")
                if last_retrieval:
                    if isinstance(last_retrieval, str):
                        last_retrieval = datetime.fromisoformat(last_retrieval)
                    if (now - last_retrieval) < TIMEOUT:
                        continue
                        
                task["last_retrieval"] = now.isoformat()
                task["current_worker_uid"] = self.uid
                available_tasks.append(task)
                if len(available_tasks) >= number:
                    break
                    
        return available_tasks

    async def _complete_tasks(self, tasks: List[Dict[str, Any]]):
        from app.utils.cache import add_completion_to_cache  # local import is OK
        for task in tasks:
            task_id = task.get("id")
            if not task_id:
                print("Skipping completed task with no 'id':", task)
                continue
            if task_id in tasks_storage:
                del tasks_storage[task_id]
            completed_tasks_storage[task_id] = task

            # Add the completed task to the cache
            conversation = task.get("conversation")
            text = task.get("text")
            model = task.get("model")  # Get the model that was used for this task
            print("adding to cache")
            if conversation and text and model:
                print("to cache 2")
                add_completion_to_cache(conversation, model, text)

        tasks_by_job: Dict[str, List[Dict[str, Any]]] = {}
        for t in completed_tasks_storage.values():
            job_name = t.get("job_name")
            if not job_name:
                continue
            tasks_by_job.setdefault(job_name, []).append(t)

        for job_name, job_tasks in tasks_by_job.items():
            await self._flush_completed_for_job(job_name, job_tasks)

    async def _handle_stream_event(self, task_id: str, event: Dict[str, Any]):
        """
        Handle a streaming event for a task.
        Events can be:
        - "chunk": Contains partial response data
        - "done": Indicates streaming is complete
        - "error": Contains error information
        """
        event_type = event.get("event")
        
        # Initialize the task's entry in completed_chunks_storage if it doesn't exist
        if task_id not in completed_chunks_storage:
            completed_chunks_storage[task_id] = []
            # Get the task info from tasks_storage to include metadata
            if task_id in tasks_storage:
                task_info = tasks_storage[task_id]
                job_name = task_info.get("job_name")
                # Mark the task as streaming in progress
                completed_chunks_storage[task_id].append({
                    "event": "start",
                    "task_id": task_id,
                    "job_name": job_name,
                    "worker_name": self.name,
                    "timestamp": datetime.now().isoformat()
                })
        
        if event_type == "chunk":
            # Store the chunk data
            chunk_data = event.get("data", {})
            chunk_with_metadata = {
                "event": "chunk",
                "task_id": task_id,
                "data": chunk_data,
                "timestamp": datetime.now().isoformat()
            }
            completed_chunks_storage[task_id].append(chunk_with_metadata)
            
            # Notify client about the new chunk immediately
            if task_id in tasks_storage:
                task = tasks_storage[task_id]
                job_name = task.get("job_name")
                await self._flush_stream_for_job(job_name, task_id)
            
        elif event_type == "done":
            # Mark the streaming as complete
            completed_chunks_storage[task_id].append({
                "event": "done",
                "task_id": task_id,
                "timestamp": datetime.now().isoformat()
            })
            print(f"[Worker {self.name}] Stream completed for task {task_id}")
            
            # Remove the task from tasks_storage since it's completed
            if task_id in tasks_storage:
                task = tasks_storage[task_id]
                job_name = task.get("job_name")
                del tasks_storage[task_id]
                
                # Notify client that streaming is complete
                await self._flush_stream_for_job(job_name, task_id)
                
        elif event_type == "error":
            # Store the error information
            error_data = event.get("data", "Unknown error")
            error_event = {
                "event": "error",
                "task_id": task_id,
                "error": error_data,
                "timestamp": datetime.now().isoformat()
            }
            completed_chunks_storage[task_id].append(error_event)
            print(f"[Worker {self.name}] Error in stream for task {task_id}: {error_data}")
            
            # Remove the task from tasks_storage since it failed
            if task_id in tasks_storage:
                task = tasks_storage[task_id]
                job_name = task.get("job_name")
                del tasks_storage[task_id]
                
                # Notify client about the error
                await self._flush_stream_for_job(job_name, task_id)

    async def _flush_stream_for_job(self, job_name: str, task_id: str):
        """
        Notify the client that streaming updates are available for a task.
        """
        if not job_name:
            print(f"[Worker {self.name}] No job name for stream task {task_id}")
            return
            
        job_info = get_or_create_job(job_name)
        client = job_info.get("client")
        
        if client:
            try:
                # This assumes client has a method to handle stream updates
                if hasattr(client, "notify_stream_update"):
                    await client.notify_stream_update(task_id)
                    
            except Exception as e:
                print(f"[Worker {self.name}] Error notifying client of stream for job '{job_name}': {e}")
        else:
            print(f"[Worker {self.name}] No client attached for job '{job_name}'")

    async def _flush_completed_for_job(self, job_name: str, tasks: List[Dict[str, Any]]):
        """
        Send completed tasks to the client and remove them from storage.
        If sending fails, re‑add the tasks to storage for later retry.
        """
        job_info = get_or_create_job(job_name)
        client = job_info.get("client")
        tasks_to_send = []
        for t in tasks:
            task_id = t.get("id")
            if task_id in completed_tasks_storage:
                tasks_to_send.append(t)
                del completed_tasks_storage[task_id]

        if client and tasks_to_send:
            try:
                ack = await client.return_tasks(tasks_to_send)
                print(f"[{self.name}] Ack for job '{job_name}': {ack}")
            except Exception as e:
                print(f"[{self.name}] Error sending tasks for job '{job_name}': {e}")
                for t in tasks_to_send:
                    task_id = t.get("id")
                    completed_tasks_storage[task_id] = t
        else:
            print(f"[{self.name}] No client attached for job '{job_name}' or no tasks to send.")

    def release_assigned_tasks(self):
        """Release tasks assigned to this worker if it disconnects."""
        for task in tasks_storage.values():
            if task.get("current_worker_uid") == self.uid:
                task.pop("current_worker_uid", None)
                task.pop("last_retrieval", None)

    async def notify_tasks_available(self):
        """Hint the worker that tasks are available."""
        if self.websocket:
            print("Notifying worker that tasks are available.")
            await self.websocket.send_json({"action": "tasks_available"})