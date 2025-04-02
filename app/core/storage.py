# app/core/storage.py

from typing import Dict, Any, List

# Existing storages
jobs_storage: Dict[str, Dict[str, Any]] = {}
tasks_storage: Dict[str, Dict[str, Any]] = {}
completed_tasks_storage: Dict[str, Dict[str, Any]] = {}
# New storage for streaming chunks
completed_chunks_storage: Dict[str, List[Dict[str, Any]]] = {}

# New in-memory storage for registered workers
workers_storage: Dict[str, Dict[str, Any]] = {}

# Historical storage for all workers that have ever connected
historical_workers_storage: Dict[str, Dict[str, Any]] = {}

# Track hot and cold models for each worker
worker_models_status: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

def get_or_create_job(job_name: str) -> Dict[str, Any]:
    if job_name not in jobs_storage:
        jobs_storage[job_name] = {
            "name": job_name,
            "client": None
        }
    return jobs_storage[job_name]

def find_job_by_client_uid(uid: str) -> Dict[str, Any]:
    for job_data in jobs_storage.values():
        client = job_data.get("client")
        if client and client.uid == uid:
            return job_data
    return {}

def save_task(task: dict, job_name: str) -> None:
    task_id = task.get("id")
    if not task_id:
        print("Task skipped due to missing 'id':", task)
        return

    if task_id in tasks_storage:
        print("DUPLICATED TASKID")

    task["job_name"] = job_name
    tasks_storage[task_id] = task
