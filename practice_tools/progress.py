"""Append-only personal attempt events; never stored in committed notebooks."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid

from .engine import ROOT


def progress_directory(spark=None):
    override = os.environ.get("DE_PRACTICE_PROGRESS_DIR")
    if override:
        path = Path(override).expanduser()
        if not path.is_absolute():
            raise ValueError("DE_PRACTICE_PROGRESS_DIR must be an absolute persistent directory.")
        return path
    if str(ROOT).startswith("/Workspace/"):
        if spark is None:
            raise ValueError("Pass spark to locate your Databricks workspace home.")
        user = spark.sql("SELECT current_user() AS user").first()["user"]
        if not user or any(c in user for c in "/\\") or user in {".", ".."}:
            raise ValueError("Cannot determine a safe workspace username.")
        home = Path("/Workspace/Users") / user
        if not home.is_dir():
            raise OSError("Workspace home not found. Set DE_PRACTICE_PROGRESS_DIR to a persistent writable workspace directory.")
        return home / ".de_interview_practice" / "events"
    return ROOT / ".practice_progress" / "events"


def record_event(exercise_id, result, *, spark=None, directory=None):
    from .engine import load_exercise
    exercise = load_exercise(exercise_id)
    if result not in {"attempted", "passed", "failed"}:
        raise ValueError("Unknown progress event.")
    folder = Path(directory) if directory is not None else progress_directory(spark)
    folder.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    event = {"version": 1, "event_id": uuid.uuid4().hex, "exercise_id": exercise["id"],
             "timestamp_utc": now, "result": result}
    # One immutable file per event avoids read-modify-write races between notebooks.
    path = folder / (event["event_id"] + ".json")
    with path.open("x", encoding="utf-8") as stream:
        json.dump(event, stream)
    return event


def read_events(*, spark=None, directory=None):
    folder = Path(directory) if directory is not None else progress_directory(spark)
    events = []
    if not folder.exists():
        return events
    for path in sorted(folder.glob("*.json")):
        try:
            event = json.loads(path.read_text(encoding="utf-8"))
            if event["version"] != 1 or event["result"] not in {"attempted", "passed", "failed"}:
                raise ValueError("Unsupported progress event")
            datetime.fromisoformat(event["timestamp_utc"])
            if not isinstance(event["exercise_id"], str) or not event["exercise_id"].isdigit():
                raise ValueError("Invalid exercise ID")
            events.append(event)
        except (ValueError, KeyError) as exc:
            raise ValueError(f"Cannot read progress file {path}; preserve it and repair/restore it. {exc}") from exc
    return sorted(events, key=lambda e: (e["timestamp_utc"], e["event_id"]))


def progress_rows(*, spark=None, directory=None):
    events = read_events(spark=spark, directory=directory)
    rows = []
    for path in sorted((ROOT / "exercises").glob("[0-9][0-9].json")):
        exercise = json.loads(path.read_text(encoding="utf-8"))
        history = [event for event in events if event["exercise_id"] == exercise["id"]]
        checks = [event for event in history if event["result"] != "attempted"]
        passed = any(event["result"] == "passed" for event in history)
        rows.append({"id": exercise["id"], "question": exercise["title"],
                     "topic": exercise["topic"], "difficulty": exercise["difficulty"],
                     "status": "Passed" if passed else "Attempted" if history else "Not attempted",
                     "checks": len(checks), "last_result": history[-1]["result"] if history else "",
                     "last_attempt_utc": history[-1]["timestamp_utc"] if history else "",
                     "notebook": f"practice/{int(exercise['id']):02d}_{exercise['slug']}.ipynb"})
    return rows


def progress_dataframe(spark, *, directory=None):
    rows = progress_rows(spark=spark, directory=directory)
    schema = ("id STRING, question STRING, topic STRING, difficulty STRING, status STRING, "
              "checks LONG, last_result STRING, last_attempt_utc STRING, notebook STRING")
    return spark.createDataFrame(rows, schema)


def mark_attempted(exercise_id, spark=None):
    event = record_event(exercise_id, "attempted", spark=spark)
    print(f"Scenario {event['exercise_id']} marked attempted; previous history preserved.")


def export_progress(path, *, spark=None):
    """Create a new backup containing raw history and dashboard rows; never overwrite."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "events": read_events(spark=spark),
               "questions": progress_rows(spark=spark)}
    with target.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2)
    return str(target)
