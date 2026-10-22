"""Create missing practice notebooks; existing attempts are never overwritten."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]

# Git-folder roots are normally on sys.path. Serverless cwd is not guaranteed,
# so also consult the notebook context and accept an explicit absolute override.
BOOTSTRAP = '''from pathlib import Path
import os
import sys

_candidates = []
if os.environ.get("DE_PRACTICE_ROOT"):
    _candidates.append(Path(os.environ["DE_PRACTICE_ROOT"]))
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _candidates.append(Path("/Workspace") / _nb_path.lstrip("/") )
except Exception:
    pass
_candidates.extend([Path.cwd()] + [Path(p) for p in sys.path if p])
_repo_root = next((parent for candidate in _candidates for parent in [candidate, *candidate.parents]
                   if (parent / "practice_tools" / "engine.py").is_file()
                   and (parent / "exercises" / "37.json").is_file()), None)
if _repo_root is None:
    raise RuntimeError("Open this notebook inside the cloned Git folder. If necessary, set "
                       "DE_PRACTICE_ROOT to that folder's absolute /Workspace/... path.")
sys.path.insert(0, str(_repo_root))
from practice_tools.notebook_setup import prepare

dbutils.widgets.text("exercise_id", "dashboard")
prepare(dbutils.widgets.get("exercise_id"), globals())
'''

LEGACY_BOOTSTRAP = BOOTSTRAP.split("from practice_tools.notebook_setup")[0] + '''from practice_tools import show_sample, check_solution, mark_attempted, progress_dataframe

if "spark" not in globals():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[2]").appName("InterviewPractice").getOrCreate()
from pyspark.sql import functions as F
from pyspark.sql.window import Window
'''


def cell(kind, source, ident):
    data = {"cell_type": kind, "id": ident, "metadata": {}, "source": source.splitlines(True)}
    if kind == "code":
        data.update(execution_count=None, outputs=[])
    return data


def notebook(cells):
    return {"cells": cells, "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}


def exercise_notebook(exercise):
    ident = exercise["id"]
    names = list(exercise["cases"][0]["inputs"])
    return notebook([
        cell("markdown", f'# {int(ident):02d}. {exercise["title"]}\n\n'
             f'**{exercise["topic"]} · {exercise["difficulty"]}**\n\n{exercise["prompt"]}\n\n'
             '**Workflow:** run setup, inspect the inputs/expected output, edit `solve`, then run the check cell. '
             'The checker uses fresh inputs for every case and never reads your `expected_df`. '
             'No reference solution is executed or included here.\n', "question"),
        cell("code", f'%run ./_setup $exercise_id="{ident}"\n', "sample-data"),
        cell("markdown", '## Your attempt\n\nReturn your result from `solve`; do not end with `.show()` or `display()`, '
             'which return `None`. Add scratch cells freely. `spark` is available; write your own imports '
             'for the PySpark functions and window utilities you need. '
             'For SQL, register the function inputs as temporary views **inside** `solve` on each call.\n\n'
             'Checking records one attempt for the whole case suite. Opening/running setup alone does not count. '
             'To record work before checking, run `mark_attempted(exercise_id, spark)` in a scratch cell.\n', "attempt-guide"),
        cell("code", f'def solve({", ".join(names)}):\n    # Write your solution here.\n    return None\n', "your-solution"),
        cell("markdown", '## Check every case\n\nRows and column positions are ignored; duplicates count. '
             'Column names and types matter (integer widths/nullability are relaxed). Arrays retain order unless '
             'the question says otherwise. Failure output shows missing/unexpected rows.\n', "check-guide"),
        cell("code", 'check_report = check_solution(exercise_id, solve, spark)\n', "check-answer"),
        cell("code", 'show_progress(exercise_id, spark)\n', "your-progress"),
    ])


def dashboard():
    return notebook([
        cell("markdown", '# Your practice dashboard\n\nRun all to refresh. **Not attempted** means no saved attempt; '
             '**Attempted** means work was marked or checked; **Passed** means at least one full suite passed. '
             '`last_result` still shows a later regression. `checks` counts checker runs, not individual test cases. '
             'Use the filters below to pick your next exercise.\n', "dashboard-guide"),
        cell("code", '%run ./_setup $exercise_id="dashboard"\n', "environment"),
        cell("code", 'from practice_tools.progress import progress_directory\n'
             'print("Progress is stored at:", progress_directory(spark))\n'
             'progress_df = progress_dataframe(spark)\n'
             'progress_df.groupBy("status").count().show()\n', "summary"),
        cell("code", '# Optional filters: leave as None to include everything.\n'
             'status_filter = None  # "Not attempted", "Attempted", or "Passed"\n'
             'topic_filter = None\n'
             'difficulty_filter = None  # "Easy", "Medium", or "Hard"\n'
             'view_df = progress_dataframe(spark)\n'
             'for name, value in [("status", status_filter), ("topic", topic_filter), ("difficulty", difficulty_filter)]:\n'
             '    if value is not None:\n        view_df = view_df.filter(view_df[name] == value)\n'
             'view_df = view_df.orderBy(view_df["id"].cast("int"))\n'
             'if "display" in globals():\n    display(view_df)\nelse:\n    view_df.show(43, truncate=False)\n', "question-list"),
        cell("markdown", '## Preserve your work\n\nIn Databricks, event files are saved outside the Git folder in '
             '`/Workspace/Users/<your-user>/.de_interview_practice/events`. Different clones in that workspace share '
             'this progress. Locally, they live in `.practice_progress/events` (ignored by Git). These locations '
             'do not sync with each other automatically.\n\nYour solution code lives in the notebook: save/commit '
             'your notebook changes before pulling conflicting updates or replacing a Git folder. '
             'The notebook builder never overwrites existing files.\n\n'
             'For a history backup, call `export_progress` with a new absolute filename in your workspace home; '
             'it refuses to overwrite an existing backup.\n', "storage-guide"),
    ])


def build(destination):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    created, kept = [], []
    specs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted((ROOT / "exercises").glob("[0-9][0-9].json"))]
    files = [(f'{int(e["id"]):02d}_{e["slug"]}.ipynb', exercise_notebook(e)) for e in specs]
    files.insert(0, ("00_Progress.ipynb", dashboard()))
    files.append(("_setup.ipynb", notebook([
        cell("markdown", '# Shared practice setup\n\nCalled by the first code cell in each exercise. '
             'Loads helper functions and fixture data without importing `F` or `Window`.\n', "setup-guide"),
        cell("code", BOOTSTRAP, "shared-environment"),
    ])))
    for name, content in files:
        path = destination / name
        try:
            with path.open("x", encoding="utf-8", newline="\n") as stream:
                json.dump(content, stream, indent=1, ensure_ascii=False)
                stream.write("\n")
            created.append(name)
        except FileExistsError:
            kept.append(name)
    return created, kept


def upgrade_layout(destination):
    """Replace only recognized generated scaffolding; never replace attempt cells."""
    updated, skipped = [], []
    for path in sorted(Path(destination).glob("*.ipynb")):
        content = json.loads(path.read_text(encoding="utf-8"))
        cells = content["cells"]
        environments = [c for c in cells if c.get("id") == "environment"]
        if not environments:
            continue
        environment = environments[0]
        if "".join(environment["source"]) != LEGACY_BOOTSTRAP:
            if "".join(environment["source"]).startswith("%run ./_setup"):
                continue
            skipped.append(path.name)
            continue
        if path.name == "00_Progress.ipynb":
            environment["source"] = ['%run ./_setup $exercise_id="dashboard"\n']
            environment.update(outputs=[], execution_count=None)
            for c in cells:
                if c.get("id") == "question-list":
                    source = "".join(c["source"])
                    source = source.replace('F.col(name)', 'view_df[name]').replace('F.col("id")', 'view_df["id"]')
                    c.update(source=source.splitlines(True), outputs=[], execution_count=None)
        else:
            ident = str(int(path.name.split("_", 1)[0]))
            exercise = json.loads((ROOT / "exercises" / f"{int(ident):02d}.json").read_text(encoding="utf-8"))
            names = list(exercise["cases"][0]["inputs"])
            old_setup = f'exercise_id = "{ident}"\ninputs, expected_output = show_sample(exercise_id, spark, globals().get("display"))\n'
            old_setup += "\n".join(f'{name} = inputs["{name}"]' for name in names) + "\n"
            expected = exercise["cases"][0]["expected"]
            if expected["kind"] == "dataframe":
                old_setup += "expected_df = expected_output\n"
            elif expected["kind"] == "outputs":
                old_setup += "\n".join(f'expected_{name}_df = expected_output["{name}"]' for name in expected["values"]) + "\n"
            samples = [c for c in cells if c.get("id") == "sample-data"]
            if len(samples) != 1 or "".join(samples[0]["source"]) != old_setup:
                skipped.append(path.name)
                continue
            samples[0].update(source=[f'%run ./_setup $exercise_id="{ident}"\n'], outputs=[], execution_count=None)
            cells.remove(environment)
            for c in cells:
                if c.get("id") == "attempt-guide":
                    text = "".join(c["source"]).replace('You can use `F`, `Window`, and `spark`. ',
                        '`spark` is available; write your own imports for the PySpark functions and window utilities you need. ')
                    c["source"] = text.splitlines(True)
                if c.get("id") == "your-progress" and "".join(c["source"]) == 'progress_dataframe(spark).filter(F.col("id") == exercise_id).show(truncate=False)\n':
                    c.update(source=['show_progress(exercise_id, spark)\n'], outputs=[], execution_count=None)
        path.write_text(json.dumps(content, indent=1, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
        updated.append(path.name)
    return updated, skipped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=ROOT / "practice",
                        help="Use a different folder to preview revised templates without touching attempts.")
    parser.add_argument("--upgrade-layout", action="store_true",
                        help="Upgrade recognized legacy setup/progress cells only; preserve solutions and scratch cells.")
    args = parser.parse_args()
    created, kept = build(args.destination)
    print(f"Created {len(created)} notebooks; preserved {len(kept)} existing files.")
    if args.upgrade_layout:
        updated, skipped = upgrade_layout(args.destination)
        print(f"Upgraded scaffolding in {len(updated)} notebooks; skipped {len(skipped)} customized layouts.")
        for name in skipped:
            print("Customized setup preserved: " + name)


if __name__ == "__main__":
    main()
