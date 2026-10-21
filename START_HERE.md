# Start practicing

## Databricks Free Edition

Create or update a Git folder using:

```text
https://github.com/RandomRishabh/interview-scenerios-spark-sql.git
```

Open **practice/00_Progress** for the dashboard, or any numbered notebook in
**practice/**. Use Python and Serverless compute. No extra packages, data upload,
SparkContext, DBFS mount, or solution notebook execution is needed there.

Run setup to display input and expected output, write your transformation in
`solve(...)`, then run the checker. **Return** your result: `.show()` and `display()`
return `None`. `F` (PySpark functions), `Window`, and `spark` are available.
Function parameter names match the displayed inputs. Add scratch cells freely.

Scenario 15 uses Python lists and a dictionary, preserving the original exercise.
Scenario 35 returns a dictionary of three DataFrames, each displayed separately.
Scenario 5 checks the transformation; partition/write discussion is ungraded.

## Checking rules

`check_solution(exercise_id, solve, spark)` loads fresh inputs for every case,
calls your function, and compares its output with stored expected data. It never
uses your notebook's `expected_df` or imports `reference_solutions`. All sample
and additional cases run on each check. These are practice tests, not secure
hidden tests: fixtures and references are readable in your repository.

- Row order and column positions are ignored. Duplicate counts matter.
- Column names are exact. Integer widths are compatible; nullability and struct
  field order are ignored. Other types must match, including original date strings.
- Nested values and nulls are checked. Array order matters unless the question
  explicitly allows unordered arrays (addresses and daily distinct products).
- Floating-point comparisons use absolute and relative tolerance of 1e-9.
- Failures show missing/unexpected rows or schema differences.
- A blank function returning `None` records no attempt. Exceptions and wrong
  outputs count as failed checks. One whole suite run counts as one check.
- Output checking cannot certify use of a particular API, ingestion mode,
  execution plan, or efficient algorithm.

For SQL, register the function's current inputs as temporary views **inside**
`solve` on every call, then return `spark.sql(your_query)`. Registering the sample
only once would leave later cases querying stale input.

See [exercise contract notes](exercises/README.md) for corrections and clarified
tie rules compared with the upstream reference solutions.

## Progress and preservation

Opening an exercise or running setup does not count as an attempt. Checking a
non-blank answer saves an event with ID, UTC time and result. To mark unfinished
work without checking, run:

```python
mark_attempted(exercise_id, spark)
```

**00_Progress** lists question, topic, difficulty, status, check count, last result,
last attempt and notebook path. Filter by status, topic or difficulty. **Passed**
means at least one full-suite pass; `last_result` still exposes later failures.
It does not infer whether you used help or import Excel-tracker progress.

In Databricks, immutable event files are stored outside the Git folder:

```text
/Workspace/Users/<current_user>/.de_interview_practice/events/
```

Other clones in the same workspace/account share this history, and compute
restarts retain it. Writing requires permission to your personal workspace
files. A failed write prints **PROGRESS NOT SAVED** without changing your check
result; it never silently falls back to ephemeral storage. A corrupt history
file raises a repair message instead of resetting progress.

Locally, history lives in `.practice_progress/events/`, ignored by Git. Local and
Databricks histories do not sync automatically. Override the location with an
absolute persistent personal directory in `DE_PRACTICE_PROGRESS_DIR` if needed.

Create a backup using a new filename:

```python
from practice_tools import export_progress
from practice_tools.progress import progress_directory
backup = progress_directory(spark).parent / "progress-backup-2026-10-21.json"
export_progress(backup, spark=spark)
```

Backups contain raw history and dashboard rows and refuse to overwrite existing
files. To restore, copy the individual event files into the destination events
directory, retaining their filenames. Do not put the combined backup file inside
`events/`.

Your answer code lives in the notebook, separately from progress. Save and
commit notebook changes before replacing the Git folder or pulling conflicting
updates. Git conflicts may still need resolving; avoid resetting your work.

## Safe notebook generation

```text
python tools/build_practice_notebooks.py
```

Creates only missing notebooks; preserves **every** existing file, including
answers, scratch cells and outputs. There is no overwrite flag. Preview revised
templates in a different folder:

```text
python tools/build_practice_notebooks.py --destination preview_notebooks
```

To intentionally reset an exercise, back up and remove that specific notebook,
then rebuild. Rebuilding never deletes progress.

## Local VS Code / Jupyter and tests

Use Python 3.11 and Java 17 for the pinned Spark 4.0.1 test runtime. Install into
a virtual environment and select its Python kernel in VS Code/Jupyter. VS Code
may also request its Python/Jupyter extensions. Databricks needs no local installs.

The pinned Spark version has a documented Windows/Python 3.12+ worker issue
([SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759)); use Python 3.11
for these local tests. This does not require changing your Databricks runtime.

```text
python -m pip install -r requirements-dev.txt
python tools/validate_notebooks.py
python -m unittest discover -s tests -v
```

Default tests cover fixture types, comparison logic, progress preservation, and
safe rebuilding. Enable real Spark tests in PowerShell:

```powershell
$env:SPARK_INTEGRATION = "1"
python -m unittest discover -s tests -v
```

On macOS/Linux: `SPARK_INTEGRATION=1 python -m unittest discover -s tests -v`.
Format validation does not execute Spark. Integration tests cover the checker
and representative solutions, not the correctness of every original reference.

## File paths on Serverless

Setup locates the Git root using notebook context and Python's import path;
Serverless current working directories are not guaranteed. If needed, set
`DE_PRACTICE_ROOT` to the absolute cloned repo path. Python reads the small JSON
fixtures and passes rows to Spark; executors never read workspace files.

Official references: [module imports](https://docs.databricks.com/aws/en/notebooks/share-code)
and [workspace file access](https://docs.databricks.com/aws/en/files/workspace-interact).
Workspace file writing must be enabled in the environment.
