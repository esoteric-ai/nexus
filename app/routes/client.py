from fastapi import APIRouter, WebSocket, HTTPException
from pydantic import BaseModel
import uuid

from app.core.client import Client
from app.core.storage import get_or_create_job, find_job_by_client_uid, jobs_storage

router = APIRouter()

class BindRequest(BaseModel):
    job_name: str

@router.get("/test")
async def test():
    # Anywhere you have access to the job/client in server code
    job_data = jobs_storage["my_test_job"]
    client = job_data["client"]

    # Ask for tasks
    tasks = await client.request_tasks(5)
    print("Got tasks:", tasks)

    # Return tasks
    ack = await client.return_tasks(tasks)
    print("Client acknowledged?", ack)


@router.post("/bind")
def bind_job(request: BindRequest):
    """
    Bind to a job (if it doesn't exist, create it) and return a client UID.
    There's only one client allowed per job in this example.
    If the client is already bound but disconnected, allow re‑binding.
    """
    job = get_or_create_job(request.job_name)
    if job["client"] is not None:
        # Allow re‑binding if the existing client is disconnected.
        if job["client"].websocket is None:
            # Reuse the existing client uid.
            return {"client_uid": job["client"].uid}
        else:
            raise HTTPException(status_code=400, detail="Job already has a client")

    # Create a unique ID for this client
    client_uid = str(uuid.uuid4())
    new_client = Client(uid=client_uid, job_name=request.job_name)
    job["client"] = new_client

    return {"client_uid": client_uid}

@router.websocket("/ws/{client_uid}")
async def websocket_endpoint(websocket: WebSocket, client_uid: str):
    """
    The client will connect here using the uid obtained from /bind.
    We'll attach the websocket to the Client object, then let
    the Client handle all inbound messages in run_forever().
    """
    await websocket.accept()

    # Find which job has this client
    job_data = find_job_by_client_uid(client_uid)
    if not job_data:
        # Not found
        await websocket.close()
        return

    client = job_data["client"]
    client.attach_websocket(websocket)

    # The client class does all the reading internally
    await client.run_forever()
