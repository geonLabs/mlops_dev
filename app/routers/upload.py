import asyncio
from datetime import datetime
import io
import mimetypes
import os
import shutil
import tempfile
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import httpx
from fastapi import APIRouter, Cookie, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.routers.cvat_webhook import CVAT_BASE_URL, SESSION_COOKIE_NAME, _get_local_session
from app.services.jobs import create_job, get_job, set_error, update_job
from app.services.minio_client import (
    MinioUploadError,
    ensure_bucket_exists,
    presign_put_object,
    put_object,
)
from app.utils.sse import sse_event_stream

router = APIRouter(prefix="/upload", tags=["upload"])

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tif", ".tiff"}
ZIP_MINIO_WORKERS = max(1, int(os.getenv("ZIP_MINIO_WORKERS", "8")))
CVAT_UPLOAD_BATCH_SIZE = max(1, int(os.getenv("CVAT_UPLOAD_BATCH_SIZE", "64")))


@dataclass(frozen=True)
class ExtractedFile:
    relative_path: str
    local_path: Path


class ZipJobCreateRequest(BaseModel):
    project_id: int = Field(gt=0)
    segment_size: int = Field(default=100, ge=1, le=50000)
    image_quality: int = Field(default=100, ge=1, le=100)
    task_name: str | None = Field(default=None, min_length=1, max_length=256)
    org: str | None = None
    org_id: int | None = Field(default=None, gt=0)



def _safe_object_name(filename: str) -> str:
    _base, ext = os.path.splitext(filename or "upload")
    return f"{uuid.uuid4().hex}{ext}"


def _is_zip(filename: str | None) -> bool:
    return bool(filename and filename.lower().endswith(".zip"))


def _is_image_file(path: Path) -> bool:
    return path.suffix.lower() in _IMAGE_EXTENSIONS


def _safe_int(value: int, *, min_value: int, max_value: int, field_name: str) -> int:
    if value < min_value or value > max_value:
        raise HTTPException(status_code=400, detail=f"{field_name} must be between {min_value} and {max_value}")
    return value


def _clamp_percent(value: int | float | None) -> int:
    if value is None:
        return 0
    try:
        num = int(value)
    except (TypeError, ValueError):
        return 0
    if num < 0:
        return 0
    if num > 100:
        return 100
    return num


def _update_zip_progress(job_id: str, **fields) -> None:
    snapshot = get_job(job_id) or {}

    upload_percent = _clamp_percent(fields.pop("upload_percent", snapshot.get("upload_percent", 0)))
    minio_percent = _clamp_percent(fields.pop("minio_percent", snapshot.get("minio_percent", 0)))
    cvat_percent = _clamp_percent(fields.pop("cvat_percent", snapshot.get("cvat_percent", 0)))

    overall_percent = int(round(upload_percent * 0.30 + minio_percent * 0.35 + cvat_percent * 0.35))

    update_job(
        job_id,
        upload_percent=upload_percent,
        minio_percent=minio_percent,
        cvat_percent=cvat_percent,
        overall_percent=overall_percent,
        **fields,
    )


def _http_error_detail(prefix: str, exc: httpx.HTTPStatusError) -> str:
    body_preview = ""
    try:
        body_preview = exc.response.text.strip()
    except Exception:
        body_preview = ""

    if body_preview:
        if len(body_preview) > 400:
            body_preview = f"{body_preview[:400]}..."
        return f"{prefix}: HTTP {exc.response.status_code} - {body_preview}"

    return f"{prefix}: HTTP {exc.response.status_code}"


def _default_task_name() -> str:
    return f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _normalize_task_name(task_name: str | None) -> str | None:
    if task_name is None:
        return None
    normalized = task_name.strip()
    if not normalized:
        return None
    return normalized[:256]


def _extract_rq_id(response: httpx.Response) -> str | None:
    try:
        payload = response.json()
    except ValueError:
        return None

    if isinstance(payload, dict):
        rq_id = payload.get("rq_id")
        if isinstance(rq_id, str) and rq_id.strip():
            return rq_id.strip()
    return None


async def _wait_for_cvat_request(
    *,
    client: httpx.AsyncClient,
    headers: dict[str, str],
    rq_id: str,
    job_id: str,
    stage: str,
    timeout_seconds: int = 900,
) -> None:
    deadline = time.time() + timeout_seconds

    while True:
        response = await client.get(f"{CVAT_BASE_URL}/api/requests/{rq_id}", headers=headers)
        response.raise_for_status()

        payload = response.json()
        status = str(payload.get("status") or "").lower()
        progress = payload.get("progress")

        if isinstance(progress, (int, float)):
            raw = float(progress)
            # Some deployments report 0~1, others 0~100. Normalize both.
            if raw > 1.0:
                raw = raw / 100.0
            pct = int(max(0.0, min(1.0, raw)) * 100)
            _update_zip_progress(job_id, stage=stage, cvat_percent=pct)

        if status == "finished":
            return
        if status == "failed":
            message = payload.get("message") or "cvat request failed"
            raise HTTPException(status_code=502, detail=f"cvat request failed ({rq_id}): {message}")

        if time.time() >= deadline:
            raise HTTPException(status_code=504, detail=f"cvat request timeout ({rq_id})")

        await asyncio.sleep(1.0)


async def _upload_to_minio(job_id: str, file: UploadFile) -> dict:
    update_job(job_id, stage="receiving")
    try:
        data = await file.read()
        object_name = f"{job_id}_{file.filename or 'upload'}"
        content_type = file.content_type or "application/octet-stream"

        ensure_bucket_exists()
        update_job(job_id, stage="server_to_minio", minio_percent=0)

        put_object(
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

        update_job(job_id, stage="done", minio_percent=100, object_name=object_name)
        return {"job_id": job_id, "object_name": object_name}
    except MinioUploadError as exc:
        set_error(job_id, str(exc))
        raise HTTPException(status_code=500, detail="minio upload failed") from exc
    except Exception as exc:
        set_error(job_id, str(exc))
        raise HTTPException(status_code=500, detail="upload failed") from exc


async def _save_upload_file(upload_file: UploadFile, destination: Path) -> int:
    total_size = 0
    with destination.open("wb") as file_obj:
        while True:
            chunk = await upload_file.read(1024 * 1024)
            if not chunk:
                break
            file_obj.write(chunk)
            total_size += len(chunk)
    return total_size


def _collect_extracted_files(extract_dir: Path) -> list[ExtractedFile]:
    entries: list[ExtractedFile] = []
    for path in extract_dir.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(extract_dir).as_posix()
        entries.append(ExtractedFile(relative_path=relative, local_path=path))

    entries.sort(key=lambda item: item.relative_path.lower())
    return entries


def _safe_extract_zip(zip_path: Path, extract_dir: Path) -> None:
    extract_root = extract_dir.resolve()
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            member_path = (extract_dir / member.filename).resolve()
            if not str(member_path).startswith(str(extract_root)):
                raise HTTPException(status_code=400, detail="zip contains unsafe path")
        zip_ref.extractall(extract_dir)


def _cvat_headers(*, session_cookies: dict[str, str], org_slug: str | None) -> dict[str, str]:
    headers = {"Accept": "application/vnd.cvat+json"}

    csrf_token = session_cookies.get("csrftoken")
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token

    if org_slug:
        headers["X-Organization"] = org_slug

    return headers


def _cvat_params(*, org_slug: str | None, org_id: int | None) -> dict[str, str | int] | None:
    params: dict[str, str | int] = {}

    if org_slug:
        params["org"] = org_slug
    elif org_id:
        params["org_id"] = org_id

    return params or None


def _chunked(items: list[ExtractedFile], size: int) -> list[list[ExtractedFile]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


async def _upload_files_to_minio_parallel(
    job_id: str,
    files: list[ExtractedFile],
    *,
    object_prefix: str,
) -> list[str]:
    ensure_bucket_exists()

    total_files = len(files)
    if total_files == 0:
        _update_zip_progress(job_id, minio_percent=100)
        return []

    max_workers = min(ZIP_MINIO_WORKERS, total_files)

    def _worker(entry: ExtractedFile) -> str:
        object_name = f"{object_prefix}/{job_id}/{entry.relative_path}"
        content_type = mimetypes.guess_type(entry.local_path.name)[0] or "application/octet-stream"

        with entry.local_path.open("rb") as file_obj:
            put_object(
                object_name=object_name,
                data=file_obj,
                length=entry.local_path.stat().st_size,
                content_type=content_type,
            )

        return object_name

    def _run_parallel() -> list[str]:
        uploaded_objects: list[str] = []
        completed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_worker, entry) for entry in files]

            for future in as_completed(futures):
                object_name = future.result()
                uploaded_objects.append(object_name)
                completed += 1
                _update_zip_progress(job_id, stage="processing", minio_percent=int(completed * 100 / total_files))

        return uploaded_objects

    return await asyncio.to_thread(_run_parallel)


async def _upload_images_to_cvat_task(
    *,
    session_cookies: dict[str, str],
    project_id: int,
    segment_size: int,
    image_quality: int,
    image_entries: list[ExtractedFile],
    task_name: str,
    org_slug: str | None,
    org_id: int | None,
    job_id: str,
    use_chunked_upload: bool = True,
) -> int:
    headers = _cvat_headers(session_cookies=session_cookies, org_slug=org_slug)
    params = _cvat_params(org_slug=org_slug, org_id=org_id)

    timeout = httpx.Timeout(connect=30.0, read=300.0, write=300.0, pool=30.0)

    async with httpx.AsyncClient(cookies=session_cookies, timeout=timeout) as client:
        task_payload = {
            "name": task_name,
            "project_id": project_id,
            "segment_size": segment_size,
        }

        create_response = await client.post(
            f"{CVAT_BASE_URL}/api/tasks",
            params=params,
            json=task_payload,
            headers=headers,
        )
        create_response.raise_for_status()

        task_data = create_response.json()
        task_id = int(task_data.get("id") or 0)
        if not task_id:
            raise HTTPException(status_code=502, detail="failed to create cvat task")

        _update_zip_progress(job_id, stage="creating_cvat_task", cvat_task_id=task_id, cvat_percent=0)

        if not use_chunked_upload:
            opened_files = []
            files_payload = []
            try:
                for entry in image_entries:
                    mime_type, _ = mimetypes.guess_type(entry.local_path.name)
                    handle = entry.local_path.open("rb")
                    opened_files.append(handle)
                    # CVAT 2.55 multipart parser expects indexed list keys (client_files[0], ...).
                    files_payload.append(
                        (
                            f"client_files[{len(files_payload)}]",
                            (
                                entry.relative_path,
                                handle,
                                mime_type or "application/octet-stream",
                            ),
                        )
                    )

                data_payload = {
                    "image_quality": str(image_quality),
                    "sorting_method": "lexicographical",
                }
                data_response = await client.post(
                    f"{CVAT_BASE_URL}/api/tasks/{task_id}/data",
                    params=params,
                    headers=headers,
                    data=data_payload,
                    files=files_payload,
                )
                data_response.raise_for_status()
                data_rq_id = _extract_rq_id(data_response)
                if data_rq_id:
                    await _wait_for_cvat_request(
                        client=client,
                        headers=headers,
                        rq_id=data_rq_id,
                        job_id=job_id,
                        stage="uploading_to_cvat",
                    )
            finally:
                for handle in opened_files:
                    handle.close()

            _update_zip_progress(job_id, cvat_percent=100)
            return task_id

        # Upload-Start 요청은 바디 없이 세션만 초기화해야 한다.
        start_headers = {**headers, "Upload-Start": "true"}
        start_response = await client.post(
            f"{CVAT_BASE_URL}/api/tasks/{task_id}/data",
            params=params,
            headers=start_headers,
        )
        start_response.raise_for_status()

        uploaded_count = 0
        total_images = len(image_entries)

        for batch in _chunked(image_entries, CVAT_UPLOAD_BATCH_SIZE):
            multi_headers = {**headers, "Upload-Multiple": "true"}
            multi_payload = {
                "image_quality": str(image_quality),
            }

            opened_files = []
            files_payload = []
            try:
                for entry in batch:
                    mime_type, _ = mimetypes.guess_type(entry.local_path.name)
                    handle = entry.local_path.open("rb")
                    opened_files.append(handle)
                    # CVAT 2.55 multipart parser expects indexed list keys (client_files[0], ...).
                    files_payload.append(
                        (
                            f"client_files[{len(files_payload)}]",
                            (
                                entry.relative_path,
                                handle,
                                mime_type or "application/octet-stream",
                            ),
                        )
                    )

                upload_response = await client.post(
                    f"{CVAT_BASE_URL}/api/tasks/{task_id}/data",
                    params=params,
                    headers=multi_headers,
                    data=multi_payload,
                    files=files_payload,
                )
                upload_response.raise_for_status()
                upload_rq_id = _extract_rq_id(upload_response)
                if upload_rq_id:
                    await _wait_for_cvat_request(
                        client=client,
                        headers=headers,
                        rq_id=upload_rq_id,
                        job_id=job_id,
                        stage="uploading_to_cvat",
                    )
            finally:
                for handle in opened_files:
                    handle.close()

            uploaded_count += len(batch)
            _update_zip_progress(
                job_id,
                stage="uploading_to_cvat",
                cvat_percent=int(uploaded_count * 100 / total_images),
            )

        finish_headers = {**headers, "Upload-Finish": "true"}
        finish_payload = {
            "image_quality": image_quality,
            "sorting_method": "lexicographical",
            "upload_file_order": [],
        }
        finish_response = await client.post(
            f"{CVAT_BASE_URL}/api/tasks/{task_id}/data",
            params=params,
            headers=finish_headers,
            json=finish_payload,
        )
        finish_response.raise_for_status()
        finish_rq_id = _extract_rq_id(finish_response)
        if finish_rq_id:
            await _wait_for_cvat_request(
                client=client,
                headers=headers,
                rq_id=finish_rq_id,
                job_id=job_id,
                stage="uploading_to_cvat",
            )

        _update_zip_progress(job_id, cvat_percent=100)

    return task_id


async def _process_zip_job(
    *,
    job_id: str,
    session_cookies: dict[str, str],
    project_id: int,
    segment_size: int,
    image_quality: int,
    task_name: str | None,
    org_slug: str | None,
    org_id: int | None,
    original_filename: str,
    zip_path: Path,
    extract_dir: Path,
    zip_size: int,
) -> None:
    try:
        _update_zip_progress(job_id, stage="extracting")
        _safe_extract_zip(zip_path, extract_dir)

        extracted_files = _collect_extracted_files(extract_dir)
        if not extracted_files:
            raise HTTPException(status_code=400, detail="zip file is empty")

        image_entries = [entry for entry in extracted_files if _is_image_file(entry.local_path)]
        if not image_entries:
            raise HTTPException(status_code=400, detail="zip does not contain supported image files")

        _update_zip_progress(job_id, stage="processing", minio_percent=0, cvat_percent=0)

        resolved_task_name = _normalize_task_name(task_name) or _default_task_name()
        _update_zip_progress(job_id, task_name=resolved_task_name)

        # MinIO에는 ZIP에서 추출된 이미지 파일만 업로드한다.
        minio_task = asyncio.create_task(
            _upload_files_to_minio_parallel(job_id, image_entries, object_prefix="zip")
        )
        cvat_task = asyncio.create_task(
            _upload_images_to_cvat_task(
                session_cookies=session_cookies,
                project_id=project_id,
                segment_size=segment_size,
                image_quality=image_quality,
                image_entries=image_entries,
                task_name=resolved_task_name,
                org_slug=org_slug,
                org_id=org_id,
                job_id=job_id,
                use_chunked_upload=False,
            )
        )

        try:
            uploaded_objects, cvat_task_id = await asyncio.gather(minio_task, cvat_task)
        except Exception:
            for task in (minio_task, cvat_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(minio_task, cvat_task, return_exceptions=True)
            raise

        _update_zip_progress(
            job_id,
            stage="done",
            upload_percent=100,
            minio_percent=100,
            cvat_percent=100,
            object_name=f"zip/{job_id}/",
            uploaded_object_count=len(uploaded_objects),
            uploaded_objects=uploaded_objects,
            zip_size=zip_size,
            extracted_file_count=len(extracted_files),
            image_file_count=len(image_entries),
            cvat_task_id=cvat_task_id,
            zip_cleanup_available=True,
        )
    except MinioUploadError as exc:
        set_error(job_id, str(exc))
    except httpx.HTTPStatusError as exc:
        set_error(job_id, _http_error_detail("cvat upload failed", exc))
    except httpx.HTTPError as exc:
        set_error(job_id, f"failed to connect cvat api: {exc}")
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, str) else "zip processing failed"
        set_error(job_id, detail)
    except Exception as exc:
        set_error(job_id, str(exc))


def _cleanup_local_zip_artifacts(zip_path_str: str | None, extract_dir_str: str | None) -> dict:
    deleted_zip = False
    deleted_extract_dir = False

    if zip_path_str:
        zip_path = Path(zip_path_str)
        if zip_path.exists() and zip_path.is_file():
            zip_path.unlink(missing_ok=True)
            deleted_zip = True

    if extract_dir_str:
        extract_dir = Path(extract_dir_str)
        if extract_dir.exists() and extract_dir.is_dir():
            shutil.rmtree(extract_dir, ignore_errors=True)
            deleted_extract_dir = True

            parent_dir = extract_dir.parent
            if parent_dir.exists() and parent_dir.is_dir():
                try:
                    parent_dir.rmdir()
                except OSError:
                    pass

    return {
        "zip_deleted": deleted_zip,
        "extract_dir_deleted": deleted_extract_dir,
    }


async def _process_image_job(
    *,
    job_id: str,
    session_cookies: dict[str, str],
    project_id: int,
    segment_size: int,
    image_quality: int,
    task_name: str | None,
    org_slug: str | None,
    org_id: int | None,
    local_path: Path,
    original_filename: str,
) -> None:
    parent_dir = local_path.parent
    try:
        image_entry = ExtractedFile(
            relative_path=Path(original_filename).name,
            local_path=local_path,
        )

        _update_zip_progress(job_id, stage="processing", minio_percent=0, cvat_percent=0)

        resolved_task_name = _normalize_task_name(task_name) or _default_task_name()
        _update_zip_progress(job_id, task_name=resolved_task_name)

        minio_task = asyncio.create_task(
            _upload_files_to_minio_parallel(job_id, [image_entry], object_prefix="image")
        )
        cvat_task = asyncio.create_task(
            _upload_images_to_cvat_task(
                session_cookies=session_cookies,
                project_id=project_id,
                segment_size=segment_size,
                image_quality=image_quality,
                image_entries=[image_entry],
                task_name=resolved_task_name,
                org_slug=org_slug,
                org_id=org_id,
                job_id=job_id,
            )
        )

        try:
            uploaded_objects, cvat_task_id = await asyncio.gather(minio_task, cvat_task)
        except Exception:
            for task in (minio_task, cvat_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(minio_task, cvat_task, return_exceptions=True)
            raise

        _update_zip_progress(
            job_id,
            stage="done",
            upload_percent=100,
            minio_percent=100,
            cvat_percent=100,
            object_name=f"image/{job_id}/",
            uploaded_object_count=len(uploaded_objects),
            uploaded_objects=uploaded_objects,
            extracted_file_count=1,
            image_file_count=1,
            cvat_task_id=cvat_task_id,
            zip_cleanup_available=False,
        )
    except MinioUploadError as exc:
        set_error(job_id, str(exc))
    except httpx.HTTPStatusError as exc:
        set_error(job_id, _http_error_detail("cvat upload failed", exc))
    except httpx.HTTPError as exc:
        set_error(job_id, f"failed to connect cvat api: {exc}")
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, str) else "image upload failed"
        set_error(job_id, detail)
    except Exception as exc:
        set_error(job_id, str(exc))
    finally:
        try:
            if local_path.exists():
                local_path.unlink(missing_ok=True)
            if parent_dir.exists():
                parent_dir.rmdir()
        except OSError:
            pass


def _init_zip_job(
    *,
    job_id: str,
    project_id: int,
    segment_size: int,
    image_quality: int,
    task_name: str | None,
    org_slug: str | None,
    org_id: int | None,
) -> None:
    normalized_task_name = _normalize_task_name(task_name)
    _update_zip_progress(
        job_id,
        stage="created",
        upload_type="zip",
        zip_cleanup_available=False,
        project_id=project_id,
        segment_size=segment_size,
        image_quality=image_quality,
        task_name=normalized_task_name,
        org=(org_slug or None),
        org_id=org_id,
        local_zip_path=None,
        local_extract_dir=None,
        uploaded_objects=[],
        uploaded_object_count=0,
        cvat_task_id=None,
    )


@router.post("/jobs")
def create_upload_job() -> dict:
    job_id = create_job()
    update_job(job_id, stage="created")
    return {"job_id": job_id}


@router.post("/presign")
def create_presigned_url(payload: dict) -> dict:
    filename = payload.get("filename") or "upload"
    object_name = _safe_object_name(filename)
    try:
        ensure_bucket_exists()
        url = presign_put_object(object_name=object_name)
        return {"object_name": object_name, "url": url}
    except MinioUploadError as exc:
        raise HTTPException(status_code=500, detail="presign failed") from exc


@router.post("/zip/jobs")
async def create_zip_job(
    payload: ZipJobCreateRequest,
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> dict:
    session = _get_local_session(cvat_session_id)
    if not session:
        raise HTTPException(status_code=401, detail="cvat login required")

    job_id = create_job()
    org_slug = (payload.org or "").strip() or None
    org_id = payload.org_id if not org_slug else None
    _init_zip_job(
        job_id=job_id,
        project_id=payload.project_id,
        segment_size=payload.segment_size,
        image_quality=payload.image_quality,
        task_name=payload.task_name,
        org_slug=org_slug,
        org_id=org_id,
    )

    return {"job_id": job_id, "ok": True}


@router.post("/image")
async def upload_image_and_create_task(
    file: UploadFile = File(...),
    project_id: int = Form(...),
    segment_size: int = Form(100),
    image_quality: int = Form(100),
    task_name: str | None = Form(None),
    org: str | None = Form(None),
    org_id: int | None = Form(None),
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> dict:
    session = _get_local_session(cvat_session_id)
    if not session:
        raise HTTPException(status_code=401, detail="cvat login required")

    _safe_int(segment_size, min_value=1, max_value=50000, field_name="segment_size")
    _safe_int(image_quality, min_value=1, max_value=100, field_name="image_quality")

    if _is_zip(file.filename):
        raise HTTPException(status_code=400, detail="zip file is not allowed on /upload/image")

    filename = file.filename or "image"
    if not _is_image_file(Path(filename)):
        raise HTTPException(status_code=400, detail="supported image file is required")

    normalized_task_name = _normalize_task_name(task_name)
    org_slug = (org or "").strip() or None
    normalized_org_id = org_id if not org_slug else None

    job_id = create_job()
    _update_zip_progress(
        job_id,
        stage="created",
        upload_type="image",
        project_id=project_id,
        segment_size=segment_size,
        image_quality=image_quality,
        task_name=normalized_task_name,
        org=org_slug,
        org_id=normalized_org_id,
        zip_cleanup_available=False,
    )

    temp_root = Path(tempfile.mkdtemp(prefix=f"mlops_cvat_image_{job_id}_"))
    local_path = temp_root / Path(filename).name

    _update_zip_progress(job_id, stage="receiving", local_extract_dir=str(temp_root))
    await _save_upload_file(file, local_path)
    await file.close()
    _update_zip_progress(job_id, stage="uploaded", upload_percent=100)

    asyncio.create_task(
        _process_image_job(
            job_id=job_id,
            session_cookies=dict(session.cookies),
            project_id=project_id,
            segment_size=segment_size,
            image_quality=image_quality,
            task_name=normalized_task_name,
            org_slug=org_slug,
            org_id=normalized_org_id,
            local_path=local_path,
            original_filename=filename,
        )
    )

    return {"job_id": job_id, "accepted": True, "message": "image received. processing started"}


@router.post("/zip/{job_id}")
async def upload_zip_to_existing_job(
    job_id: str,
    file: UploadFile = File(...),
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> dict:
    session = _get_local_session(cvat_session_id)
    if not session:
        raise HTTPException(status_code=401, detail="cvat login required")

    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    if job.get("upload_type") != "zip":
        raise HTTPException(status_code=400, detail="not a zip upload job")

    if job.get("stage") not in {"created", "error"}:
        raise HTTPException(status_code=409, detail="job already submitted")

    if not _is_zip(file.filename):
        raise HTTPException(status_code=400, detail="zip file is required")

    project_id = int(job.get("project_id") or 0)
    segment_size = int(job.get("segment_size") or 100)
    image_quality = int(job.get("image_quality") or 100)
    task_name = _normalize_task_name(job.get("task_name"))
    org_slug = (job.get("org") or "").strip() or None
    org_id = job.get("org_id")
    org_id = int(org_id) if org_id and not org_slug else None

    _safe_int(segment_size, min_value=1, max_value=50000, field_name="segment_size")
    _safe_int(image_quality, min_value=1, max_value=100, field_name="image_quality")

    temp_root = Path(tempfile.mkdtemp(prefix=f"mlops_cvat_zip_{job_id}_"))
    zip_path = temp_root / (file.filename or "upload.zip")
    extract_dir = temp_root / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    _update_zip_progress(
        job_id,
        stage="receiving",
        local_zip_path=str(zip_path),
        local_extract_dir=str(extract_dir),
        zip_cleanup_available=False,
        upload_percent=0,
        minio_percent=0,
        cvat_percent=0,
    )

    zip_size = await _save_upload_file(file, zip_path)
    await file.close()

    if not zipfile.is_zipfile(zip_path):
        set_error(job_id, "invalid zip file")
        raise HTTPException(status_code=400, detail="invalid zip file")

    _update_zip_progress(job_id, stage="uploaded", upload_percent=100)

    asyncio.create_task(
        _process_zip_job(
            job_id=job_id,
            session_cookies=dict(session.cookies),
            project_id=project_id,
            segment_size=segment_size,
            image_quality=image_quality,
            task_name=task_name,
            org_slug=org_slug,
            org_id=org_id,
            original_filename=file.filename or "upload.zip",
            zip_path=zip_path,
            extract_dir=extract_dir,
            zip_size=zip_size,
        )
    )

    return {
        "job_id": job_id,
        "accepted": True,
        "message": "zip received. processing started",
    }


@router.post("/zip")
async def upload_zip_and_create_task_legacy(
    file: UploadFile = File(...),
    project_id: int = Form(...),
    segment_size: int = Form(100),
    image_quality: int = Form(100),
    task_name: str | None = Form(None),
    org: str | None = Form(None),
    org_id: int | None = Form(None),
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> dict:
    session = _get_local_session(cvat_session_id)
    if not session:
        raise HTTPException(status_code=401, detail="cvat login required")

    _safe_int(segment_size, min_value=1, max_value=50000, field_name="segment_size")
    _safe_int(image_quality, min_value=1, max_value=100, field_name="image_quality")

    org_slug = (org or "").strip() or None
    org_id = org_id if not org_slug else None

    job_id = create_job()
    _init_zip_job(
        job_id=job_id,
        project_id=project_id,
        segment_size=segment_size,
        image_quality=image_quality,
        task_name=task_name,
        org_slug=org_slug,
        org_id=org_id,
    )

    if not _is_zip(file.filename):
        raise HTTPException(status_code=400, detail="zip file is required")

    # keep compatibility: accept single call and enqueue background processing
    return await upload_zip_to_existing_job(
        job_id=job_id,
        file=file,
        cvat_session_id=cvat_session_id,
    )


@router.delete("/zip/{job_id}")
def delete_zip_artifacts(job_id: str) -> dict:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    if job.get("upload_type") != "zip":
        raise HTTPException(status_code=400, detail="not a zip upload job")

    result = _cleanup_local_zip_artifacts(
        zip_path_str=job.get("local_zip_path"),
        extract_dir_str=job.get("local_extract_dir"),
    )

    update_job(
        job_id,
        local_zip_path=None,
        local_extract_dir=None,
        zip_cleanup_available=False,
        zip_deleted=True,
    )

    return {
        "ok": True,
        "job_id": job_id,
        **result,
    }


@router.post("/{job_id}")
async def upload_file(job_id: str, file: UploadFile = File(...)) -> dict:
    if not get_job(job_id):
        raise HTTPException(status_code=404, detail="job not found")
    return await _upload_to_minio(job_id, file)


@router.post("")
async def upload_file_auto_job(file: UploadFile = File(...)) -> dict:
    job_id = create_job()
    update_job(job_id, stage="created")
    return await _upload_to_minio(job_id, file)


@router.get("/{job_id}")
def get_upload_job(job_id: str) -> dict:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@router.get("/{job_id}/events")
def stream_upload_events(job_id: str):
    def _snapshot():
        job = get_job(job_id)
        if not job:
            return {"job_id": job_id, "stage": "error", "error": "job not found"}
        return job

    return StreamingResponse(
        sse_event_stream(_snapshot),
        media_type="text/event-stream",
    )
