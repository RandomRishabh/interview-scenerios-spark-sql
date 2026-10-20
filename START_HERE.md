# Spark interview practice in Databricks

This fork retains the original 43 scenario reference files and datasets from
[Mohan Krishna's repository](https://github.com/mohankrishna02/interview-scenerios-spark-sql).
The reference files contain solutions. For a fresh attempt, create a separate
notebook, copy the input-data setup, and write your own transformation below it.

## Open the whole repository

1. In Databricks Free Edition, open **Workspace** and your personal folder.
2. Choose **Create > Git folder**.
3. Paste `https://github.com/RandomRishabh/interview-scenerios-spark-sql.git`.
4. Select **GitHub**, leave sparse checkout off, and create the Git folder.
5. Open any scenario and select **Serverless** compute.

Both `.ipynb` files and the `.py` files are notebooks: Python files include the
Databricks source-notebook header. Scenario 37 is now a valid Jupyter notebook.
The original scenario names are retained so existing links and numbering remain useful.

## Compatibility changes

- Scenarios 1–29 open as Databricks Python source notebooks.
- Legacy Spark startup reuses Databricks' existing `spark` session. Outside
  Databricks, the affected files can create a local session when PySpark and Java
  are installed. Local Spark execution is not configured by cloning this repository.
- Scenarios 19, 20, and 25 include their small bundled fixtures in the code, so
  they do not depend on the original author's Windows paths or DBFS FileStore.
- Scenario 25 explicitly rejects CSV rows with a mismatched field count.
- Scenario 5 reports observed **non-empty** partition IDs using DataFrame APIs;
  this differs from counting all physical partitions with the RDD API.
- Scenario 5's Parquet write is optional. Set `output_path` to your own writable
  Unity Catalog volume to practice writing. It is skipped by default.
- Missing `Window` imports, scenario 33's nonexistent aggregate column,
  scenario 38's `emp_name` header typo, and scenario 40's date-offset type are fixed.

The retained Scala/Maven files are upstream reference material, not entry points
for Free Edition, which uses Python/SQL serverless notebooks.

## Validate locally

In an environment with Python installed:

```text
python -m pip install -r requirements-dev.txt
python tools/validate_notebooks.py
```

This checks the schema of all 14 Jupyter notebooks, the Python syntax of every
scenario, Databricks headers on the 29 Python notebooks, and bundled JSON syntax.
It does **not** execute the Spark transformations or verify every reference
solution's business logic. Run a chosen scenario in Databricks to check its result.

## References

- [Create Databricks Git folders](https://docs.databricks.com/aws/en/repos/git-operations-with-repos)
- [Notebook recognition and formats](https://docs.databricks.com/aws/en/notebooks/notebook-export-import)
- [Serverless API and storage limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
