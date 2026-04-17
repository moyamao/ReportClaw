from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def load_last_crawl_ts(path: Path) -> datetime | None:
    """Load crawler watermark from the shared JSON state file.

    Shared schema examples:
    - last_crawl_end_iso: used by `reportclaw.main`
    - last_sent_iso / last_generated_iso: used by `reportclaw.daily_report`

    Backward compatibility:
    - legacy key `last_end_iso` is still accepted.
    """
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return None

        raw = obj.get("last_crawl_end_iso") or obj.get("last_end_iso")
        if not raw:
            return None

        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(float(raw))

        raw_s = str(raw).strip()
        if len(raw_s) == 10:
            return datetime.strptime(raw_s, "%Y-%m-%d")
        return datetime.fromisoformat(raw_s)
    except Exception:
        return None


def save_last_crawl_ts(path: Path, dt: datetime) -> None:
    """Persist crawler watermark without overwriting other shared state keys."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        obj: dict = {}
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    old = json.load(f)
                if isinstance(old, dict):
                    obj.update(old)
            except Exception:
                obj = {}

        obj["last_crawl_end_iso"] = dt.isoformat(timespec="seconds")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
