import base64
import os
import secrets
import time
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from threading import Lock
from urllib.parse import urljoin

import httpx
from fastapi import APIRouter, Cookie, HTTPException, Request, Response
from pydantic import BaseModel, Field

from google.auth.transport.requests import Request as GRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

router = APIRouter(tags=["cvat"])

CVAT_BASE_URL = os.getenv("CVAT_BASE_URL", "http://125.142.22.24:52000")
CVAT_USER = os.getenv("CVAT_USER", "master")
CVAT_PASS = os.getenv("CVAT_PASS", "eogksxhddnsits008!")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "pjmsm0319@geonspace.com")

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CREDENTIALS_PATH = Path(os.getenv("GMAIL_CREDENTIALS_PATH", str(PROJECT_ROOT / "credentials.json")))
TOKEN_PATH = Path(os.getenv("GMAIL_TOKEN_PATH", str(PROJECT_ROOT / "token.json")))

SESSION_COOKIE_NAME = "cvat_session_id"
SESSION_TTL_SECONDS = int(os.getenv("CVAT_SESSION_TTL_SECONDS", "28800"))
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"


@dataclass
class CVATSession:
    username: str
    cookies: dict[str, str]
    created_at: float
    updated_at: float


_CVAT_SESSIONS: dict[str, CVATSession] = {}
_CVAT_SESSIONS_LOCK = Lock()


class CVATLoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


def _purge_expired_sessions() -> None:
    now = time.time()
    expired = [
        session_id
        for session_id, session in _CVAT_SESSIONS.items()
        if now - session.updated_at > SESSION_TTL_SECONDS
    ]
    for session_id in expired:
        _CVAT_SESSIONS.pop(session_id, None)


def _create_local_session(*, username: str, cookies: dict[str, str]) -> str:
    session_id = secrets.token_urlsafe(32)
    now = time.time()
    with _CVAT_SESSIONS_LOCK:
        _purge_expired_sessions()
        _CVAT_SESSIONS[session_id] = CVATSession(
            username=username,
            cookies=dict(cookies),
            created_at=now,
            updated_at=now,
        )
    return session_id


def _get_local_session(session_id: str | None) -> CVATSession | None:
    if not session_id:
        return None

    with _CVAT_SESSIONS_LOCK:
        _purge_expired_sessions()
        session = _CVAT_SESSIONS.get(session_id)
        if not session:
            return None
        session.updated_at = time.time()
        return session


def _delete_local_session(session_id: str | None) -> CVATSession | None:
    if not session_id:
        return None

    with _CVAT_SESSIONS_LOCK:
        return _CVAT_SESSIONS.pop(session_id, None)


async def _login_to_cvat(*, username: str, password: str) -> dict[str, str]:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        csrf_token = None
        try:
            csrf_response = await client.get(f"{CVAT_BASE_URL}/api/auth/csrf")
            if csrf_response.is_success:
                csrf_token = csrf_response.cookies.get("csrftoken") or client.cookies.get("csrftoken")
        except httpx.HTTPError:
            csrf_token = None

        headers = {"Accept": "application/vnd.cvat+json"}
        if csrf_token:
            headers["X-CSRFToken"] = csrf_token

        response = await client.post(
            f"{CVAT_BASE_URL}/api/auth/login",
            json={"username": username, "password": password},
            headers=headers,
        )
        response.raise_for_status()

        cookies = dict(client.cookies)
        if not cookies:
            cookies = dict(response.cookies)

        if not cookies:
            raise HTTPException(status_code=502, detail="cvat login succeeded but no session cookie returned")

        return cookies


def gmail_service():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(GRequest())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)

        with TOKEN_PATH.open("w", encoding="utf-8") as file_obj:
            file_obj.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


def send_mail(subject: str, body: str, to_email: str):
    msg = MIMEText(body, _charset="utf-8")
    msg["to"] = to_email
    msg["subject"] = subject

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    service = gmail_service()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()


async def all_jobs_completed(task_id: int) -> bool:
    # webhook flow keeps server-side credentials
    async with httpx.AsyncClient(auth=(CVAT_USER, CVAT_PASS), timeout=30) as client:
        response = await client.get(f"{CVAT_BASE_URL}/api/jobs", params={"task_id": task_id})
        response.raise_for_status()
        data = response.json()
        jobs = data.get("results", data)

        if not jobs:
            return False

        return all((job.get("state") == "completed") for job in jobs)


async def list_projects_in_org(
    cookies: dict[str, str],
    org_slug: str | None,
    org_id: int | None,
) -> list[dict]:
    async def _list_once(
        *,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        query_org: str | None,
        query_org_id: int | None,
    ) -> list[dict]:
        projects = []
        next_url = f"{CVAT_BASE_URL}/api/projects?page_size=100"
        first_request = True

        while next_url:
            params = None
            if first_request:
                params = {}
                if query_org:
                    params["org"] = query_org
                elif query_org_id:
                    params["org_id"] = query_org_id
                if not params:
                    params = None
            response = await client.get(next_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            first_request = False

            if isinstance(data, list):
                results = data
                next_url = None
            else:
                results = data.get("results", [])
                next_page = data.get("next")
                if next_page and next_page.startswith("/"):
                    next_url = urljoin(CVAT_BASE_URL, next_page)
                else:
                    next_url = next_page

            for project in results:
                projects.append(
                    {
                        "id": project.get("id"),
                        "name": project.get("name"),
                        "organization": project.get("organization"),
                    }
                )
        return projects

    base_headers = {"Accept": "application/vnd.cvat+json"}
    strategies: list[tuple[dict[str, str], str | None, int | None]]
    if org_slug:
        # Prefer slug context and avoid combining slug + org_id in one request.
        strategies = [
            ({**base_headers, "X-Organization": org_slug}, org_slug, None),
            ({**base_headers, "X-Organization": org_slug}, None, None),
            (base_headers, org_slug, None),
        ]
    elif org_id:
        strategies = [(base_headers, None, org_id)]
    else:
        strategies = [(base_headers, None, None)]

    async with httpx.AsyncClient(cookies=cookies, timeout=30) as client:
        for headers, query_org, query_org_id in strategies:
            try:
                projects = await _list_once(
                    client=client,
                    headers=headers,
                    query_org=query_org,
                    query_org_id=query_org_id,
                )
            except httpx.HTTPStatusError as exc:
                # Compatibility fallback for deployments rejecting one of the context styles.
                if exc.response.status_code in (400, 404) and (org_slug or org_id):
                    continue
                raise

            if projects:
                return projects

    return []


async def list_organizations(cookies: dict[str, str]) -> list[dict]:
    organizations = []
    next_url = f"{CVAT_BASE_URL}/api/organizations?page_size=100"

    async with httpx.AsyncClient(cookies=cookies, timeout=30) as client:
        while next_url:
            response = await client.get(next_url, headers={"Accept": "application/vnd.cvat+json"})
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                results = data
                next_url = None
            else:
                results = data.get("results", [])
                next_page = data.get("next")
                if next_page and next_page.startswith("/"):
                    next_url = urljoin(CVAT_BASE_URL, next_page)
                else:
                    next_url = next_page

            for org in results:
                organizations.append(
                    {
                        "id": org.get("id"),
                        "slug": org.get("slug"),
                        "name": org.get("name"),
                    }
                )

    return organizations


@router.post("/cvat/auth/login")
async def cvat_auth_login(payload: CVATLoginRequest, response: Response):
    try:
        cookies = await _login_to_cvat(username=payload.username, password=payload.password)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (400, 401, 403):
            raise HTTPException(status_code=401, detail="invalid cvat credentials") from exc
        raise HTTPException(status_code=exc.response.status_code, detail="failed to login cvat") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="failed to connect cvat") from exc

    session_id = _create_local_session(username=payload.username, cookies=cookies)
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite="lax",
        secure=SESSION_COOKIE_SECURE,
    )

    return {"ok": True, "username": payload.username}


@router.post("/cvat/auth/logout")
async def cvat_auth_logout(
    response: Response,
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    session = _delete_local_session(cvat_session_id)

    if session:
        try:
            csrf_token = session.cookies.get("csrftoken")
            headers = {"Accept": "application/vnd.cvat+json"}
            if csrf_token:
                headers["X-CSRFToken"] = csrf_token

            async with httpx.AsyncClient(cookies=session.cookies, timeout=30) as client:
                await client.post(f"{CVAT_BASE_URL}/api/auth/logout", headers=headers)
        except httpx.HTTPError:
            pass

    response.delete_cookie(key=SESSION_COOKIE_NAME, samesite="lax")
    return {"ok": True}


@router.get("/cvat/auth/me")
async def cvat_auth_me(
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    session = _get_local_session(cvat_session_id)
    if not session:
        return {"authenticated": False}

    return {
        "authenticated": True,
        "username": session.username,
    }


@router.get("/cvat/projects")
async def cvat_projects(
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
    org: str | None = None,
    org_id: int | None = None,
):
    session = _get_local_session(cvat_session_id)
    if not session:
        raise HTTPException(status_code=401, detail="cvat login required")

    org_slug = (org or "").strip() or None

    try:
        projects = await list_projects_in_org(session.cookies, org_slug=org_slug, org_id=org_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403):
            _delete_local_session(cvat_session_id)
            raise HTTPException(status_code=401, detail="cvat session expired. login again") from exc
        raise HTTPException(status_code=exc.response.status_code, detail="failed to fetch cvat projects") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="failed to connect cvat api") from exc

    projects.sort(key=lambda project: (project.get("name") or "").lower())
    return {"count": len(projects), "results": projects}


@router.get("/cvat/organizations")
async def cvat_organizations(
    cvat_session_id: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    session = _get_local_session(cvat_session_id)
    if not session:
        raise HTTPException(status_code=401, detail="cvat login required")

    try:
        organizations = await list_organizations(session.cookies)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403):
            _delete_local_session(cvat_session_id)
            raise HTTPException(status_code=401, detail="cvat session expired. login again") from exc
        raise HTTPException(status_code=exc.response.status_code, detail="failed to fetch cvat organizations") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="failed to connect cvat api") from exc

    organizations.sort(key=lambda org_item: (org_item.get("name") or "").lower())
    return {"count": len(organizations), "results": organizations}


@router.post("/cvat/webhook")
async def cvat_webhook(req: Request):
    payload = await req.json()
    event = payload.get("event")

    if event != "update:job":
        return {"ok": True, "ignored": True, "reason": "not job event"}

    job = payload.get("job") or {}
    task_id = job.get("task_id")
    job_id = job.get("id")

    if not task_id:
        return {"ok": True, "ignored": True, "reason": "no task_id"}

    if await all_jobs_completed(int(task_id)):
        subject = f"[CVAT] Task {task_id} all jobs completed"
        body = f"Task ID: {task_id}\nTriggered by Job ID: {job_id}\n"
        send_mail(subject, body, ADMIN_EMAIL)
        return {"ok": True, "mailed": True, "task_id": task_id}

    return {"ok": True, "mailed": False, "task_id": task_id}
