// Same-origin backend (served by this FastAPI app)
const API_BASE = "";

const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("fileInput");
const pickBtn = document.getElementById("pickBtn");
const startBtn = document.getElementById("startBtn");
const clearBtn = document.getElementById("clearBtn");
const listEl = document.getElementById("list");
const summaryEl = document.getElementById("summary");
const overallUpload = document.getElementById("overallUpload");
const overallUploadText = document.getElementById("overallUploadText");
const overallMinio = document.getElementById("overallMinio");
const overallMinioText = document.getElementById("overallMinioText");

const authState = document.getElementById("authState");
const openLoginBtn = document.getElementById("openLoginBtn");
const logoutBtn = document.getElementById("logoutBtn");
const loginModal = document.getElementById("loginModal");
const closeLoginBtn = document.getElementById("closeLoginBtn");
const cancelLoginBtn = document.getElementById("cancelLoginBtn");
const submitLoginBtn = document.getElementById("submitLoginBtn");
const loginForm = document.getElementById("loginForm");
const loginUsername = document.getElementById("loginUsername");
const loginPassword = document.getElementById("loginPassword");
const loginError = document.getElementById("loginError");

const orgSelect = document.getElementById("orgSelect");
const projectSelect = document.getElementById("projectSelect");
const refreshProjectsBtn = document.getElementById("refreshProjectsBtn");
const projectHint = document.getElementById("projectHint");
const segmentSizeInput = document.getElementById("segmentSizeInput");
const imageQualityInput = document.getElementById("imageQualityInput");
const taskNameAutoTab = document.getElementById("taskNameAutoTab");
const taskNameCustomTab = document.getElementById("taskNameCustomTab");
const taskNameAutoPanel = document.getElementById("taskNameAutoPanel");
const taskNameCustomPanel = document.getElementById("taskNameCustomPanel");
const taskNamePreview = document.getElementById("taskNamePreview");
const taskNameCustomInput = document.getElementById("taskNameCustomInput");

const queue = [];
const items = [];
let active = 0;
const MAX_CONCURRENT = 2;
let totalBytes = 0;
let running = false;

let isAuthenticated = false;
let currentUser = "";
let taskNameMode = "auto";

function isZipFile(file) {
  return file.name.toLowerCase().endsWith(".zip");
}

function isImageFile(file) {
  const name = (file?.name || "").toLowerCase();
  return [".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tif", ".tiff"].some((ext) => name.endsWith(ext));
}

function selectedOrgContext() {
  const selected = orgSelect.options[orgSelect.selectedIndex] || null;
  const slug = orgSelect.value ? orgSelect.value.trim() : "";
  const orgId = selected && selected.dataset && selected.dataset.orgId ? selected.dataset.orgId.trim() : "";
  const label = selected ? selected.textContent : "개인 워크스페이스";
  return { slug, orgId, label };
}

function setAuthState(authenticated, username = "") {
  isAuthenticated = authenticated;
  currentUser = username || "";

  if (authenticated) {
    authState.innerHTML = `<strong>${currentUser}</strong> 로 CVAT 로그인됨`;
    openLoginBtn.hidden = true;
    logoutBtn.hidden = false;
  } else {
    authState.textContent = "CVAT 로그인이 필요합니다.";
    openLoginBtn.hidden = false;
    logoutBtn.hidden = true;
  }
}

function openLoginModal() {
  loginError.textContent = "";
  loginModal.classList.remove("hidden");
  setTimeout(() => loginUsername.focus(), 0);
}

function closeLoginModal() {
  loginModal.classList.add("hidden");
  loginForm.reset();
  loginError.textContent = "";
}

function setProjectHint(message, isError = false) {
  projectHint.textContent = message;
  projectHint.classList.toggle("error", isError);
}

function renderOrganizationOptions(organizations) {
  orgSelect.innerHTML = "";

  const personalOption = document.createElement("option");
  personalOption.value = "";
  personalOption.dataset.orgId = "";
  personalOption.textContent = "개인 워크스페이스";
  orgSelect.appendChild(personalOption);

  organizations.forEach((organization) => {
    const option = document.createElement("option");
    option.value = organization.slug ?? "";
    option.dataset.orgId = organization.id != null ? String(organization.id) : "";

    const slugText = organization.slug ? ` (${organization.slug})` : "";
    option.textContent = `${organization.name ?? "(이름 없음)"}${slugText}`;

    orgSelect.appendChild(option);
  });
}

function renderProjectOptions(projects) {
  projectSelect.innerHTML = "";

  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = projects.length ? "프로젝트 선택" : "프로젝트 없음";
  projectSelect.appendChild(defaultOption);

  projects.forEach((project) => {
    const option = document.createElement("option");
    option.value = String(project.id ?? "");
    option.textContent = `${project.name ?? "(이름 없음)"} (#${project.id ?? "-"})`;
    projectSelect.appendChild(option);
  });
}

function clearOrganizations() {
  orgSelect.innerHTML = '<option value="">개인 워크스페이스</option>';
  orgSelect.disabled = true;
}

function clearProjects(reasonText) {
  projectSelect.innerHTML = '<option value="">프로젝트 선택</option>';
  projectSelect.disabled = true;
  if (reasonText) setProjectHint(reasonText);
}

async function checkAuthSession() {
  try {
    const response = await fetch(`${API_BASE}/cvat/auth/me`, {
      credentials: "same-origin",
    });

    if (!response.ok) {
      setAuthState(false);
      return;
    }

    const payload = await response.json();
    if (payload.authenticated) {
      setAuthState(true, payload.username || "user");
    } else {
      setAuthState(false);
    }
  } catch (_) {
    setAuthState(false);
  }
}

function selectedOrgLabel() {
  return selectedOrgContext().label;
}

async function loadOrganizations() {
  if (!isAuthenticated) {
    clearOrganizations();
    clearProjects("CVAT 로그인 후 프로젝트를 불러올 수 있습니다.");
    return;
  }

  const prev = selectedOrgContext();
  orgSelect.disabled = true;
  orgSelect.innerHTML = '<option value="">조직 불러오는 중...</option>';

  try {
    const response = await fetch(`${API_BASE}/cvat/organizations`, {
      credentials: "same-origin",
    });

    if (response.status === 401) {
      setAuthState(false);
      clearOrganizations();
      clearProjects("세션이 만료되었습니다. 다시 로그인해주세요.");
      return;
    }

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = await response.json();
    const organizations = Array.isArray(payload) ? payload : payload.results || [];
    renderOrganizationOptions(organizations);
    orgSelect.disabled = false;

    const availableSlugs = organizations
      .map((organization) => (organization.slug || "").trim())
      .filter((slug) => slug.length > 0);

    if (prev.slug && availableSlugs.includes(prev.slug)) {
      orgSelect.value = prev.slug;
    } else if (availableSlugs.length > 0) {
      orgSelect.value = availableSlugs[0];
    } else {
      orgSelect.value = "";
    }

    await loadProjects();
  } catch (err) {
    clearOrganizations();
    orgSelect.disabled = false;
    setProjectHint(`조직 조회 실패, 개인 워크스페이스로 조회합니다: ${err}`, true);
    await loadProjects();
  }
}

async function loadProjects() {
  if (!isAuthenticated) {
    clearProjects("CVAT 로그인 후 프로젝트를 불러올 수 있습니다.");
    return;
  }

  const org = selectedOrgContext();

  projectSelect.disabled = true;
  refreshProjectsBtn.disabled = true;
  projectSelect.innerHTML = '<option value="">프로젝트 불러오는 중...</option>';
  setProjectHint(`${selectedOrgLabel()} 기준 프로젝트 목록을 불러오는 중입니다.`);

  try {
    const params = new URLSearchParams();
    if (org.slug) {
      params.set("org", org.slug);
    } else if (org.orgId) {
      params.set("org_id", org.orgId);
    }

    const query = params.toString() ? `?${params.toString()}` : "";
    const response = await fetch(`${API_BASE}/cvat/projects${query}`, {
      credentials: "same-origin",
    });

    if (response.status === 401) {
      setAuthState(false);
      clearProjects("세션이 만료되었습니다. 다시 로그인해주세요.");
      return;
    }

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = await response.json();
    const projects = Array.isArray(payload) ? payload : payload.results || [];

    renderProjectOptions(projects);
    projectSelect.disabled = projects.length === 0;
    setProjectHint(
      projects.length
        ? `${selectedOrgLabel()} 기준 ${projects.length}개 프로젝트를 불러왔습니다.`
        : `${selectedOrgLabel()} 기준 프로젝트가 없습니다.`
    );
  } catch (err) {
    projectSelect.innerHTML = '<option value="">불러오기 실패</option>';
    projectSelect.disabled = true;
    setProjectHint(`프로젝트 조회 실패: ${err}`, true);
  } finally {
    refreshProjectsBtn.disabled = false;
  }
}

async function loginToCvat(username, password) {
  const response = await fetch(`${API_BASE}/cvat/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    body: JSON.stringify({ username, password }),
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.detail || `HTTP ${response.status}`);
  }

  setAuthState(true, payload.username || username);
}

async function logoutFromCvat() {
  await fetch(`${API_BASE}/cvat/auth/logout`, {
    method: "POST",
    credentials: "same-origin",
  });
  setAuthState(false);
  clearOrganizations();
  clearProjects("CVAT 로그인 후 프로젝트를 불러올 수 있습니다.");
}

function formatSize(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  return `${mb.toFixed(1)} MB`;
}

function updateSummary() {
  const done = items.filter((i) => i.status === "done").length;
  const errored = items.filter((i) => i.status === "error").length;
  const queued = items.filter((i) => i.status === "queued").length;
  summaryEl.textContent = `${active}개 업로드 중 · ${queued}개 대기 · ${done}개 완료 · ${errored}개 실패`;
}

function updateOverall() {
  if (!totalBytes) {
    overallUpload.value = 0;
    overallUploadText.textContent = "0%";
    overallMinio.value = 0;
    overallMinioText.textContent = "대기중";
    return;
  }

  const totalLoaded = items.reduce((sum, i) => sum + (i.loaded || 0), 0);
  const uploadPercent = Math.min(100, Math.round((totalLoaded / totalBytes) * 100));
  overallUpload.value = uploadPercent;
  overallUploadText.textContent = `${uploadPercent}%`;

  const doneCount = items.filter((i) => i.status === "done").length;
  const donePercent = items.length ? Math.round((doneCount / items.length) * 100) : 0;
  overallMinio.value = donePercent;
  overallMinioText.textContent = `${donePercent}%`;
}

function createItem(file) {
  const el = document.createElement("div");
  el.className = "item";
  el.innerHTML = `
    <div class="item-head">
      <div class="item-title" data-name>${file.name}</div>
      <div class="badge">${formatSize(file.size)}</div>
    </div>
    <div class="status">대기중</div>
    <div class="item-progress-row">
      <progress data-progress max="100" value="0"></progress>
      <span class="item-progress-text" data-progress-text>0%</span>
    </div>
    <div class="item-actions"></div>
  `;
  listEl.prepend(el);
  return el;
}

function setItemProgress(item, percent) {
  const progressEl = item.itemEl.querySelector("[data-progress]");
  const progressTextEl = item.itemEl.querySelector("[data-progress-text]");
  if (!progressEl || !progressTextEl) return;

  const value = Math.max(0, Math.min(100, Math.round(percent)));
  progressEl.value = value;
  progressTextEl.textContent = `${value}%`;

  item.loaded = Math.round((item.file.size * value) / 100);
  updateOverall();
}

function enqueueFiles(files) {
  Array.from(files).forEach((file) => {
    const itemEl = createItem(file);
    const item = {
      file,
      itemEl,
      status: "queued",
      loaded: 0,
      minioPercent: 0,
      jobId: null,
      isZip: isZipFile(file),
      isImage: isImageFile(file),
    };
    items.push(item);
    queue.push(item);
    totalBytes += file.size;
    setItemProgress(item, 0);
  });
  updateSummary();
  updateOverall();
  startBtn.disabled = items.length === 0;
}

function pumpQueue() {
  while (active < MAX_CONCURRENT && queue.length > 0) {
    const task = queue.shift();
    startUpload(task).catch(() => {});
  }
  updateSummary();
  updateOverall();
  if (active === 0 && queue.length === 0) {
    running = false;
    startBtn.disabled = items.length === 0;
  } else {
    startBtn.disabled = true;
  }
}

function uploadToPresignedUrl(file, presignedUrl, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", presignedUrl, true);
    xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable && onProgress) {
        onProgress(event.loaded, event.total);
      }
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve();
        return;
      }
      reject(new Error(`HTTP ${xhr.status}`));
    };

    xhr.onerror = () => reject(new Error("network error"));
    xhr.send(file);
  });
}

function uploadZipFileToJob(jobId, file, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append("file", file);

    xhr.open("POST", `${API_BASE}/upload/zip/${encodeURIComponent(jobId)}`, true);
    xhr.withCredentials = true;

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable && onProgress) {
        onProgress(event.loaded, event.total);
      }
    };

    xhr.onload = () => {
      let payload = {};
      try {
        payload = JSON.parse(xhr.responseText || "{}");
      } catch (_) {
        payload = {};
      }

      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(payload);
        return;
      }

      reject(new Error(payload.detail || `HTTP ${xhr.status}`));
    };

    xhr.onerror = () => reject(new Error("network error"));
    xhr.send(formData);
  });
}

function uploadImageToCvatAndMinio(file, options, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const formData = new FormData();
    formData.append("file", file);
    formData.append("project_id", String(options.projectId));
    formData.append("segment_size", String(options.segmentSize));
    formData.append("image_quality", String(options.imageQuality));
    if (options.taskName) {
      formData.append("task_name", options.taskName);
    }
    if (options.orgSlug) {
      formData.append("org", options.orgSlug);
    } else if (options.orgId) {
      formData.append("org_id", String(options.orgId));
    }

    xhr.open("POST", `${API_BASE}/upload/image`, true);
    xhr.withCredentials = true;

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable && onProgress) {
        onProgress(event.loaded, event.total);
      }
    };

    xhr.onload = () => {
      let payload = {};
      try {
        payload = JSON.parse(xhr.responseText || "{}");
      } catch (_) {
        payload = {};
      }

      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(payload);
        return;
      }

      reject(new Error(payload.detail || `HTTP ${xhr.status}`));
    };

    xhr.onerror = () => reject(new Error("network error"));
    xhr.send(formData);
  });
}

function validateZipOptions() {
  const segmentSize = Number(segmentSizeInput.value || 0);
  const imageQuality = Number(imageQualityInput.value || 0);

  if (!Number.isInteger(segmentSize) || segmentSize < 1) {
    throw new Error("Segment Size는 1 이상의 정수여야 합니다.");
  }
  if (!Number.isInteger(imageQuality) || imageQuality < 1 || imageQuality > 100) {
    throw new Error("Image Quality는 1~100 사이 정수여야 합니다.");
  }

  return { segmentSize, imageQuality };
}

function buildAutoTaskName() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mm = String(now.getMinutes()).padStart(2, "0");
  const ss = String(now.getSeconds()).padStart(2, "0");
  return `task_${y}${m}${d}_${hh}${mm}${ss}`;
}

function refreshTaskNamePreview() {
  if (!taskNamePreview) return;
  taskNamePreview.textContent = buildAutoTaskName();
}

function setTaskNameMode(mode) {
  taskNameMode = mode === "custom" ? "custom" : "auto";

  if (!taskNameAutoTab || !taskNameCustomTab || !taskNameAutoPanel || !taskNameCustomPanel) {
    return;
  }

  if (taskNameMode === "auto") {
    taskNameAutoTab.classList.add("active");
    taskNameCustomTab.classList.remove("active");
    taskNameAutoTab.setAttribute("aria-selected", "true");
    taskNameCustomTab.setAttribute("aria-selected", "false");
    taskNameAutoPanel.classList.remove("hidden");
    taskNameCustomPanel.classList.add("hidden");
    refreshTaskNamePreview();
    return;
  }

  taskNameAutoTab.classList.remove("active");
  taskNameCustomTab.classList.add("active");
  taskNameAutoTab.setAttribute("aria-selected", "false");
  taskNameCustomTab.setAttribute("aria-selected", "true");
  taskNameAutoPanel.classList.add("hidden");
  taskNameCustomPanel.classList.remove("hidden");
  if (taskNameCustomInput) {
    setTimeout(() => taskNameCustomInput.focus(), 0);
  }
}

function resolveTaskName() {
  if (taskNameMode === "custom") {
    const value = (taskNameCustomInput?.value || "").trim();
    if (!value) {
      throw new Error("직접 입력 모드에서는 Task Name을 입력해주세요.");
    }
    if (value.length > 256) {
      throw new Error("Task Name은 256자를 넘길 수 없습니다.");
    }
    return value;
  }
  return buildAutoTaskName();
}

async function cleanupZipJob(item, button, statusEl) {
  if (!item.jobId) return;

  button.disabled = true;
  button.textContent = "삭제 중...";

  try {
    const response = await fetch(`${API_BASE}/upload/zip/${encodeURIComponent(item.jobId)}`, {
      method: "DELETE",
      credentials: "same-origin",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.detail || `HTTP ${response.status}`);
    }

    button.textContent = "ZIP 임시파일 삭제됨";
    statusEl.textContent = `${statusEl.textContent} · 임시 ZIP 삭제 완료`;
  } catch (err) {
    button.disabled = false;
    button.textContent = "ZIP 임시파일 삭제";
    statusEl.textContent = `ZIP 삭제 실패: ${err.message || err}`;
  }
}

function attachZipCleanupAction(item, statusEl) {
  if (!item.jobId) return;

  const actionsEl = item.itemEl.querySelector(".item-actions");
  if (!actionsEl || actionsEl.querySelector(".zip-cleanup-btn")) return;

  const button = document.createElement("button");
  button.type = "button";
  button.className = "ghost zip-cleanup-btn";
  button.textContent = "ZIP 임시파일 삭제";
  button.addEventListener("click", () => {
    cleanupZipJob(item, button, statusEl).catch(() => {});
  });
  actionsEl.appendChild(button);
}

function watchZipJob(item, statusEl, nameEl) {
  if (!item.jobId) {
    return Promise.reject(new Error("job id is missing"));
  }

  return new Promise((resolve, reject) => {
    const events = new EventSource(`${API_BASE}/upload/${encodeURIComponent(item.jobId)}/events`);

    events.onmessage = (event) => {
      let payload = {};
      try {
        payload = JSON.parse(event.data || "{}");
      } catch (_) {
        return;
      }

      const stage = payload.stage || "unknown";
      const uploadType = payload.upload_type || "";
      const overall = Number(payload.overall_percent || 0);
      const minioPercent = Number(payload.minio_percent || 0);
      const cvatPercent = Number(payload.cvat_percent || 0);

      if (Number.isFinite(overall) && overall >= 0) {
        setItemProgress(item, overall);
      }

      if (stage === "uploaded") {
        statusEl.textContent =
          uploadType === "zip"
            ? "서버 수신 완료. 압축 해제 및 업로드 준비 중..."
            : "서버 수신 완료. 업로드 처리 준비 중...";
      } else if (stage === "extracting") {
        statusEl.textContent = "ZIP 압축 해제 중...";
      } else if (stage === "creating_cvat_task") {
        statusEl.textContent = "CVAT Task 생성 중...";
      } else if (stage === "uploading_to_cvat" || stage === "processing") {
        statusEl.textContent = `처리 중 · MinIO ${minioPercent}% · CVAT ${cvatPercent}%`;
      } else if (stage === "done") {
        item.status = "done";
        setItemProgress(item, 100);
        const taskId = payload.cvat_task_id;
        nameEl.textContent = `${item.file.name} → CVAT Task #${taskId ?? "-"}`;
        statusEl.textContent = `완료 · Task #${taskId ?? "-"} · MinIO ${payload.uploaded_object_count ?? 0} files`;

        if (payload.zip_cleanup_available) {
          attachZipCleanupAction(item, statusEl);
        }

        events.close();
        resolve(payload);
      } else if (stage === "error") {
        events.close();
        reject(new Error(payload.error || "zip processing failed"));
      }
    };

    events.onerror = () => {
      // keep waiting; SSE reconnect is automatic
    };
  });
}

async function createZipJob(projectId, segmentSize, imageQuality, taskName, orgSlug, orgId) {
  const requestPayload = {
    project_id: Number(projectId),
    segment_size: segmentSize,
    image_quality: imageQuality,
    task_name: taskName,
    org: orgSlug || null,
    org_id: null,
  };

  if (!requestPayload.org && orgId) {
    requestPayload.org_id = Number(orgId);
  }

  const response = await fetch(`${API_BASE}/upload/zip/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    body: JSON.stringify(requestPayload),
  });

  const payload = await response.json().catch(() => ({}));

  if (response.status === 401) {
    setAuthState(false);
    clearOrganizations();
    clearProjects("세션이 만료되었습니다. 다시 로그인해주세요.");
    throw new Error(payload.detail || "CVAT 로그인 세션이 만료되었습니다.");
  }

  if (!response.ok) {
    throw new Error(payload.detail || `HTTP ${response.status}`);
  }

  return payload;
}

async function startZipUpload(item, statusEl, nameEl) {
  if (!isAuthenticated) {
    throw new Error("ZIP 업로드는 CVAT 로그인 후 가능합니다.");
  }

  const projectId = projectSelect.value ? projectSelect.value.trim() : "";
  if (!projectId) {
    throw new Error("ZIP 업로드 전에 CVAT 프로젝트를 선택해주세요.");
  }

  const { segmentSize, imageQuality } = validateZipOptions();
  const taskName = resolveTaskName();
  const org = selectedOrgContext();

  statusEl.textContent = "ZIP 작업 생성 중...";

  const jobPayload = await createZipJob(projectId, segmentSize, imageQuality, taskName, org.slug, org.orgId);
  item.jobId = jobPayload.job_id;

  const watchPromise = watchZipJob(item, statusEl, nameEl);
  watchPromise.catch(() => {});

  statusEl.textContent = "ZIP 파일을 서버로 업로드 중...";
  await uploadZipFileToJob(item.jobId, item.file, (loaded, total) => {
    const percent = Math.round((loaded / total) * 25);
    setItemProgress(item, percent);
    statusEl.textContent = `ZIP 파일 서버 업로드 중... ${Math.round((loaded / total) * 100)}%`;
  });

  statusEl.textContent = "서버 수신 완료. 백엔드 처리 상태를 기다리는 중...";
  setItemProgress(item, 30);

  await watchPromise;
}

async function startImageUpload(item, statusEl, nameEl) {
  if (!isAuthenticated) {
    throw new Error("이미지 CVAT 업로드는 CVAT 로그인 후 가능합니다.");
  }

  const projectId = projectSelect.value ? projectSelect.value.trim() : "";
  if (!projectId) {
    throw new Error("이미지 업로드 전에 CVAT 프로젝트를 선택해주세요.");
  }

  const { segmentSize, imageQuality } = validateZipOptions();
  const taskName = resolveTaskName();
  const org = selectedOrgContext();

  statusEl.textContent = "이미지 파일을 서버로 업로드 중...";
  const payload = await uploadImageToCvatAndMinio(
    item.file,
    {
      projectId,
      segmentSize,
      imageQuality,
      taskName,
      orgSlug: org.slug,
      orgId: org.orgId,
    },
    (loaded, total) => {
      const percent = Math.round((loaded / total) * 25);
      setItemProgress(item, percent);
      statusEl.textContent = `이미지 파일 서버 업로드 중... ${Math.round((loaded / total) * 100)}%`;
    }
  );

  item.jobId = payload.job_id;
  const watchPromise = watchZipJob(item, statusEl, nameEl);
  watchPromise.catch(() => {});

  statusEl.textContent = "서버 수신 완료. 백엔드 처리 상태를 기다리는 중...";
  setItemProgress(item, 30);
  await watchPromise;
}

async function startDirectUpload(item, statusEl, nameEl) {
  const projectId = projectSelect.value ? projectSelect.value.trim() : "";
  if (item.isImage && isAuthenticated) {
    if (!projectId) {
      throw new Error("이미지 CVAT 업로드를 하려면 CVAT 프로젝트를 선택해주세요.");
    }
    await startImageUpload(item, statusEl, nameEl);
    return;
  }

  statusEl.textContent = "프리사인 URL 생성중...";

  const presignResponse = await fetch(`${API_BASE}/upload/presign`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filename: item.file.name,
      content_type: item.file.type || "application/octet-stream",
    }),
  });

  const presignPayload = await presignResponse.json().catch(() => ({}));
  if (!presignResponse.ok) {
    throw new Error(presignPayload.detail || `presign failed (${presignResponse.status})`);
  }

  const objectName = presignPayload.object_name;
  const presignedUrl = presignPayload.url;

  await uploadToPresignedUrl(item.file, presignedUrl, (loaded, total) => {
    const percent = Math.round((loaded / total) * 100);
    statusEl.textContent = `브라우저 → MinIO 업로드 중... ${percent}%`;
    setItemProgress(item, percent);
  });

  item.status = "done";
  nameEl.textContent = `${item.file.name} → ${objectName}`;
  statusEl.textContent = "완료";
  setItemProgress(item, 100);
}

async function startUpload(item) {
  const { itemEl } = item;
  const statusEl = itemEl.querySelector(".status");
  const nameEl = itemEl.querySelector("[data-name]");

  active += 1;
  item.status = "uploading";
  updateSummary();

  try {
    if (item.isZip) {
      await startZipUpload(item, statusEl, nameEl);
    } else {
      await startDirectUpload(item, statusEl, nameEl);
    }
  } catch (err) {
    item.status = "error";
    statusEl.textContent = `업로드 실패: ${err.message || err}`;
  } finally {
    active -= 1;
    pumpQueue();
  }
}

openLoginBtn.addEventListener("click", openLoginModal);
closeLoginBtn.addEventListener("click", closeLoginModal);
cancelLoginBtn.addEventListener("click", closeLoginModal);

loginModal.addEventListener("click", (event) => {
  if (event.target === loginModal) closeLoginModal();
});

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !loginModal.classList.contains("hidden")) {
    closeLoginModal();
  }
});

loginForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  loginError.textContent = "";
  submitLoginBtn.disabled = true;

  const username = loginUsername.value.trim();
  const password = loginPassword.value;

  try {
    await loginToCvat(username, password);
    closeLoginModal();
    await loadOrganizations();
  } catch (err) {
    loginError.textContent = `로그인 실패: ${err.message || err}`;
  } finally {
    submitLoginBtn.disabled = false;
  }
});

logoutBtn.addEventListener("click", () => {
  logoutFromCvat().catch(() => {
    setAuthState(false);
    clearOrganizations();
    clearProjects("CVAT 로그인 후 프로젝트를 불러올 수 있습니다.");
  });
});

orgSelect.addEventListener("change", () => {
  loadProjects().catch(() => {});
});

refreshProjectsBtn.addEventListener("click", () => {
  loadProjects().catch(() => {});
});

projectSelect.addEventListener("change", () => {
  const selected = projectSelect.options[projectSelect.selectedIndex];
  if (selected && selected.value) {
    setProjectHint(`선택된 프로젝트: ${selected.textContent}`);
    return;
  }
  if (isAuthenticated) {
    setProjectHint("프로젝트를 선택하지 않았습니다.");
  } else {
    setProjectHint("CVAT 로그인 후 프로젝트를 불러올 수 있습니다.");
  }
});

if (taskNameAutoTab && taskNameCustomTab) {
  taskNameAutoTab.addEventListener("click", () => setTaskNameMode("auto"));
  taskNameCustomTab.addEventListener("click", () => setTaskNameMode("custom"));
}

if (taskNameCustomInput) {
  taskNameCustomInput.addEventListener("input", () => {
    if (taskNameMode !== "custom") return;
    taskNameCustomInput.value = taskNameCustomInput.value.slice(0, 256);
  });
}

pickBtn.addEventListener("click", () => fileInput.click());
dropzone.addEventListener("click", (event) => {
  if (event.target && event.target.closest("button")) return;
  fileInput.click();
});
fileInput.addEventListener("change", (event) => enqueueFiles(event.target.files));

startBtn.addEventListener("click", () => {
  if (!items.length || running) return;
  running = true;
  pumpQueue();
});

clearBtn.addEventListener("click", () => {
  listEl.innerHTML = "";
  queue.length = 0;
  items.length = 0;
  totalBytes = 0;
  active = 0;
  running = false;
  startBtn.disabled = true;
  updateSummary();
  updateOverall();
});

["dragenter", "dragover"].forEach((eventName) => {
  dropzone.addEventListener(eventName, (event) => {
    event.preventDefault();
    event.stopPropagation();
    dropzone.classList.add("dragover");
  });
});

["dragleave", "drop"].forEach((eventName) => {
  dropzone.addEventListener(eventName, (event) => {
    event.preventDefault();
    event.stopPropagation();
    dropzone.classList.remove("dragover");
  });
});

dropzone.addEventListener("drop", (event) => {
  const dt = event.dataTransfer;
  if (dt && dt.files && dt.files.length) {
    enqueueFiles(dt.files);
  }
});

async function init() {
  updateSummary();
  updateOverall();
  startBtn.disabled = true;
  setTaskNameMode("auto");
  refreshTaskNamePreview();

  await checkAuthSession();
  if (isAuthenticated) {
    await loadOrganizations();
  } else {
    clearOrganizations();
    clearProjects("CVAT 로그인 후 프로젝트를 불러올 수 있습니다.");
  }
}

init().catch(() => {
  setAuthState(false);
  clearOrganizations();
  clearProjects("CVAT 로그인 후 프로젝트를 불러올 수 있습니다.");
});
