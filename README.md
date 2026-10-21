# Spark interview practice

**Start in [`practice/00_Progress.ipynb`](practice/00_Progress.ipynb).**

43 exercises with input data, expected output, a blank solution cell, and an
automatic checker. No practice notebook contains or executes a reference solution.

1. Open this repository as a **Git folder** in Databricks Free Edition.
2. Open a notebook in **practice/** and select **Serverless**.
3. Run the setup cells to see the input and expected output.
4. Write `solve(...)`, returning your result, and run the check cell.
5. Refresh **00_Progress** to see Not attempted / Attempted / Passed.

| Folder | Purpose |
| --- | --- |
| [practice/](practice/) | Your notebooks and progress dashboard |
| [exercises/](exercises/) | Problem statements, typed inputs, expected rows, extra cases |
| [practice_tools/](practice_tools/) | Shared loader, checker, and personal progress storage |
| [reference_solutions/](reference_solutions/) | Original 43 notebooks, retained separately; contains spoilers |
| [tools/](tools/) | Notebook builder and validation |
| [tests/](tests/) | Checker, progress preservation, and Spark integration tests |

Your progress is saved automatically when you check a non-blank solution. The
dashboard records checker-run counts and the latest result. Passing once keeps
the status **Passed**, while a later failed run remains visible in `last_result`.
To record work before checking, run `mark_attempted(exercise_id, spark)`.

Read [START_HERE.md](START_HERE.md) for progress storage, SQL use, backups, local
setup, and tests. Read [exercise contract notes](exercises/README.md) for the
clarifications made to ambiguous upstream examples.

Based on [Mohan Krishna's original scenarios](https://github.com/mohankrishna02/interview-scenerios-spark-sql).
Original reference code is preserved. Practice contracts and fixtures are
maintained separately and may correct ambiguities in those references.
