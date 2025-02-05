# nexus/app/core/client.py

import asyncio
import uuid
from typing import Optional, Dict, Any
from fastapi import WebSocket, WebSocketDisconnect

from app.core.storage import get_or_create_job, save_task, workers_storage, tasks_storage, completed_tasks_storage
from app.utils.cache import get_cached_completion

class Client:
    """
    Represents a connected client for a given job.
    The client object is long‑lived so that even if the websocket disconnects,
    the same Client object is reused upon reconnection.
    """
    def __init__(self, uid: str, job_name: str):
        self.uid: str = uid
        self.job_name: str = job_name
        self.websocket: Optional[WebSocket] = None

        # When a websocket is attached, we set this event.
        self.connected_event = asyncio.Event()

        # Pending requests from server to client: request_id -> asyncio.Future
        self.pending_requests: Dict[str, asyncio.Future] = {}

    def attach_websocket(self, websocket: WebSocket):
        """Attach (or re‑attach) the websocket."""
        self.websocket = websocket
        self.connected_event.set()
        print(f"Client {self.uid} attached a websocket.")

    async def run_forever(self):
        """
        Main loop reading messages. On disconnect, simply clear the websocket
        so that the same client object can be re‑attached.
        """
        if not self.websocket:
            raise ConnectionError("No WebSocket attached to client.")

        try:
            while True:
                print("receiving json")
                data = await self.websocket.receive_json()
                await self.handle_incoming_message(data)
        except WebSocketDisconnect:
            print(f"Client {self.uid} disconnected.")
        #except Exception as e:
        #    print(f"Client {self.uid} encountered error: {e}. Ignoring.")
        finally:
            self.websocket = None
            self.connected_event.clear()

    async def handle_incoming_message(self, data: dict):
        print(data)
        """
        Process incoming messages.
          - "response_tasks" and "ack_returned" resolve pending futures.
          - "submit_tasks" saves tasks and notifies workers.
          - **New:** "start_job" clears any previous tasks for this job and instructs workers to cancel in‑progress tasks.
        """
        action = data.get("action")
        request_id = data.get("request_id")

        if action == "response_tasks":
            tasks = data.get("tasks", [])
            future = self.pending_requests.get(request_id)
            if future and not future.done():
                future.set_result(tasks)
                del self.pending_requests[request_id]

        elif action == "ack_returned":
            ack = data.get("ack", False)
            future = self.pending_requests.get(request_id)
            if future and not future.done():
                future.set_result(ack)
                del self.pending_requests[request_id]

        elif action == "submit_tasks":
            tasks = data.get("tasks", [])
            tasks_to_queue = []
            cached_tasks = []

            for task in tasks:
                conversation = task.get("conversation")
                models = task.get("models", [])

                if conversation and models:
                    cached = get_cached_completion(conversation, models)
                    print("trying to hit cache")
                    if cached:
                        print("cache hit")
                        # Use the cached result but update the 'id' so that the client's
                        # pending future (keyed by task id) is resolved correctly.
                        cached_result = cached.copy()
                        cached_result["id"] = task.get("id")
                        cached_tasks.append(cached_result)
                        continue

                # If no cached result, save the task as usual.
                save_task(task, self.job_name)
                tasks_to_queue.append(task)

            # Instead of awaiting return_tasks (which blocks the receive loop),
            # schedule it as a background task so that the ack can be processed.
            if cached_tasks:
                asyncio.create_task(self.return_tasks(cached_tasks))

            # For tasks that are not cached, notify all workers that tasks are available.
            if tasks_to_queue:
                for worker_info in workers_storage.values():
                    worker = worker_info["worker"]
                    await worker.notify_tasks_available()

        # === NEW CODE FOR JOB RESTART ===
        elif action == "start_job":
            # The client explicitly wants to restart the job.
            job_name = self.job_name
            print(f"Client {self.uid} requested to restart job '{job_name}'.")

            # Find and remove all tasks in the main tasks storage for this job.
            tasks_to_cancel = [
                task for task in list(tasks_storage.values())
                if task.get("job_name") == job_name
            ]

            print("To cancel:" + str(len(tasks_to_cancel)))
            print("All tasks:" + str(len(list(tasks_storage.values()))))
            # Gather the worker IDs that have been assigned tasks for this job.
            worker_ids = set()
            for task in tasks_to_cancel:
                worker_uid = task.get("current_worker_uid")
                if worker_uid:
                    worker_ids.add(worker_uid)
                # Delete the task from storage.
                task_id = task.get("id")
                if task_id in tasks_storage:
                    del tasks_storage[task_id]

            print("Worker ids:" + str(worker_ids))

            print("Completed to remove:" + str(len(list(completed_tasks_storage.items()))))

            # Also clear any completed tasks for this job.
            for task_id, task in list(completed_tasks_storage.items()):
                if task.get("job_name") == job_name:
                    del completed_tasks_storage[task_id]

            print("After removal all tasks:" + str(len(list(tasks_storage.values()))))
            print("After removal completed:" + str(len(list(completed_tasks_storage.items()))))

            # Notify each worker that had tasks for this job to cancel them.
            for worker_uid in worker_ids:
                worker_info = workers_storage.get(worker_uid)
                if worker_info:
                    worker = worker_info["worker"]
                    print("Notifying worker.")
                    if worker.websocket:
                        try:
                            await worker.websocket.send_json({
                                "action": "cancel_tasks",
                                "job_name": job_name,
                            })
                            print(f"Sent cancel_tasks to worker {worker_uid} for job '{job_name}'.")
                        except Exception as e:
                            print(f"Error sending cancel_tasks to worker {worker_uid}: {e}")

            # Optionally, if this "start_job" request was sent with a request_id, send an ack.
            if request_id:
                await self.websocket.send_json({
                    "action": "ack_start_job",
                    "request_id": request_id,
                    "ack": True
                })

        else:
            print("Received unhandled message:", data)

    async def return_tasks(self, tasks: list):
        """
        Send a 'return_tasks' message to the client and await an ack.
        If the client is disconnected, wait until it reconnects.
        A timeout is used so that we do not wait forever.
        """
        # Wait for the websocket to be (re‑)attached.
        while not self.websocket:
            print(f"Client {self.uid} not connected; waiting to send tasks...")
            await self.connected_event.wait()

        request_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[request_id] = future

        # Attempt to send the tasks.
        try:
            await self.websocket.send_json({
                "action": "return_tasks",
                "request_id": request_id,
                "tasks": tasks
            })
            print("send task")
        except Exception as send_exc:
            if request_id in self.pending_requests:
                self.pending_requests[request_id].set_exception(send_exc)
                del self.pending_requests[request_id]
            raise send_exc

        # Wait for an ack with a timeout.
        try:
            print("waiting for ack")
            ack = await asyncio.wait_for(future, timeout=30)  # 30-second timeout.
            print("ack is here")
            return ack
        except asyncio.TimeoutError as te:
            if request_id in self.pending_requests:
                del self.pending_requests[request_id]
            print(f"Timeout waiting for ack from client {self.uid}")
            raise te
