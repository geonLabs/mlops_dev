import json
import time
from typing import Callable, Dict, Any, Iterator

def sse_event_stream(get_snapshot: Callable[[], Dict[str, Any]]) -> Iterator[str]:
    """
    get_snapshot()이 반환하는 dict를 JSON으로 보내는 SSE 제너레이터.
    변경될 때만 전송.
    """
    last = None
    while True:
        snap = get_snapshot()
        key = (
            snap.get("updated_at"),
            snap.get("stage"),
            snap.get("upload_percent"),
            snap.get("minio_percent"),
            snap.get("cvat_percent"),
            snap.get("overall_percent"),
            snap.get("object_name"),
            snap.get("error"),
        )
        if key != last:
            yield f"data: {json.dumps(snap, ensure_ascii=False)}\n\n"
            last = key

        if snap.get("stage") in ("done", "error"):
            return

        time.sleep(0.2)
