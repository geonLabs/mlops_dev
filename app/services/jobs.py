import time
import uuid
import threading
from typing import Any, Dict, Optional

_JOBS: Dict[str, Dict[str, Any]] = {}
_LOCK = threading.Lock()

def create_job() -> str:
    job_id = uuid.uuid4().hex
    with _LOCK:
        _JOBS[job_id] = {
            "job_id": job_id,
            "stage": "created",          # created | receiving | server_to_minio | done | error
            "minio_percent": 0,
            "object_name": None,
            "error": None,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
    return job_id

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with _LOCK:
        job = _JOBS.get(job_id)
        return dict(job) if job else None

def update_job(job_id: str, **fields) -> None:
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return
        job.update(fields)
        job["updated_at"] = time.time()

def set_error(job_id: str, msg: str) -> None:
    update_job(job_id, stage="error", error=msg)
