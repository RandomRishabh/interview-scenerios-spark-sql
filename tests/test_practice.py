"""Unit tests; opt into local Spark with SPARK_INTEGRATION=1."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from practice_tools import engine, progress


def execute_cell(source, namespace):
    """Emulate only the documented shared %run for local integration testing."""
    if source.startswith('%run ./_setup $exercise_id="'):
        ident = source.split('"')[1]
        namespace["dbutils"] = SimpleNamespace(widgets=SimpleNamespace(
            text=lambda name, default: None, get=lambda name: ident))
        shared = json.loads((engine.ROOT / "practice/_setup.ipynb").read_text(encoding="utf-8"))
        for cell in shared["cells"]:
            if cell["cell_type"] == "code":
                exec("".join(cell["source"]), namespace)
    else:
        exec(source, namespace)


class PracticeTests(unittest.TestCase):
    def test_all_fixtures_have_cases_and_valid_typed_rows(self):
        from pyspark.sql.types import StructType, _make_type_verifier
        def validate(spec):
            if spec["kind"] == "dataframe":
                schema = StructType.fromJson(spec["schema"])
                verify = _make_type_verifier(schema)
                for row in spec["rows"]:
                    self.assertEqual(set(row), set(schema.names))
                    verify(row)
            elif spec["kind"] == "outputs":
                for part in spec["values"].values():
                    validate(part)
            else:
                self.assertEqual(spec["kind"], "python")
                self.assertIn("value", spec)
        for ident in map(str, range(1, 44)):
            ex = engine.load_exercise(ident)
            self.assertEqual(ex["id"], ident)
            self.assertGreaterEqual(len(ex["cases"]), 3)
            for case in ex["cases"]:
                for value in case["inputs"].values():
                    validate(value)
                validate(case["expected"])

    def test_matching_preserves_duplicates_and_handles_tolerance_overlap(self):
        self.assertEqual(engine._matching([1, 1], [1], lambda a, b: a == b), ([1], []))
        self.assertEqual(engine._matching([1, 2], [2, 1], lambda a, b: a == b), ([], []))
        self.assertEqual(engine._matching([1, 2], [2, 3], lambda a, b: abs(a-b) <= 1), ([], []))

    def test_array_order_only_relaxed_for_declared_columns(self):
        a, b = {"products": ["A", "B"]}, {"products": ["B", "A"]}
        self.assertFalse(engine._equal(a, b, set()))
        self.assertTrue(engine._equal(a, b, {"products"}))
        self.assertFalse(engine._equal({"products": ["A", "A"]}, b, {"products"}))
        self.assertFalse(engine._equal(True, 1, set()))

    def test_progress_survives_new_instances_and_later_failures(self):
        with tempfile.TemporaryDirectory() as folder:
            rows = progress.progress_rows(directory=folder)
            self.assertEqual(len(rows), 43)
            self.assertTrue(all(r["status"] == "Not attempted" for r in rows))
            progress.record_event("37", "attempted", directory=folder)
            progress.record_event("37", "failed", directory=folder)
            progress.record_event("37", "passed", directory=folder)
            progress.record_event("37", "failed", directory=folder)
            row = progress.progress_rows(directory=folder)[36]
            self.assertEqual((row["status"], row["checks"], row["last_result"]), ("Passed", 3, "failed"))
            self.assertEqual(len(list(Path(folder).glob("*.json"))), 4)

    def test_corrupt_progress_is_not_silently_reset(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "broken.json"
            path.write_text("{", encoding="utf-8")
            with self.assertRaises(ValueError):
                progress.progress_rows(directory=folder)
            self.assertTrue(path.exists())

    def test_interrupted_progress_write_does_not_poison_history(self):
        with tempfile.TemporaryDirectory() as folder:
            progress.record_event("37", "passed", directory=folder)
            def interrupted_dump(event, stream):
                stream.write("{")
                stream.flush()
                # A concurrent dashboard refresh sees only the published event.
                self.assertEqual(len(progress.read_events(directory=folder)), 1)
                raise OSError("simulated interrupted write")
            with patch.object(progress.json, "dump", side_effect=interrupted_dump):
                with self.assertRaises(OSError):
                    progress.record_event("37", "failed", directory=folder)
            self.assertEqual(len(progress.read_events(directory=folder)), 1)
            self.assertEqual(progress.progress_rows(directory=folder)[36]["status"], "Passed")
            self.assertEqual(len(list(Path(folder).iterdir())), 1)

    def test_backup_refuses_to_overwrite(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {"DE_PRACTICE_PROGRESS_DIR": folder}):
            target = Path(folder).parent / (Path(folder).name + "-backup.json")
            try:
                progress.record_event("1", "failed")
                progress.export_progress(target)
                with self.assertRaises(FileExistsError):
                    progress.export_progress(target)
                self.assertEqual(len(progress.read_events()), 1)
            finally:
                target.unlink(missing_ok=True)

    def test_builder_keeps_modified_notebooks_and_progress(self):
        spec = importlib.util.spec_from_file_location("builder", engine.ROOT / "tools/build_practice_notebooks.py")
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        with tempfile.TemporaryDirectory() as folder:
            created, kept = builder.build(folder)
            self.assertEqual((len(created), len(kept)), (45, 0))
            path = Path(folder) / "37_latest_order.ipynb"
            path.write_text("my saved work", encoding="utf-8")
            created, kept = builder.build(folder)
            self.assertEqual((len(created), len(kept)), (0, 45))
            self.assertEqual(path.read_text(), "my saved work")

    def test_shared_layout_does_not_inject_learning_imports(self):
        import ast
        for path in (engine.ROOT / "practice").glob("*.ipynb"):
            notebook = json.loads(path.read_text(encoding="utf-8"))
            scaffold_ids = {"sample-data", "environment", "shared-environment", "your-progress", "question-list", "summary"}
            code = ["".join(c["source"]) for c in notebook["cells"]
                    if c["cell_type"] == "code" and c.get("id") in scaffold_ids]
            if path.name != "_setup.ipynb":
                self.assertTrue(code[0].startswith("%run ./_setup"))
                self.assertEqual(len(code[0].strip().splitlines()), 1)
            for source in code:
                if source.startswith("%run"):
                    continue
                tree = ast.parse(source)
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom):
                        self.assertFalse(any(a.asname == "F" or a.name == "Window" for a in node.names))

    def test_layout_migration_preserves_solution_and_scratch(self):
        spec = importlib.util.spec_from_file_location("builder", engine.ROOT / "tools/build_practice_notebooks.py")
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        with tempfile.TemporaryDirectory() as folder:
            builder.build(folder)
            path = Path(folder) / "37_latest_order.ipynb"
            content = json.loads(path.read_text(encoding="utf-8"))
            solution = next(c for c in content["cells"] if c.get("id") == "your-solution")
            solution.update(source=["def solve(input_df):\n    return input_df\n"], execution_count=9,
                            outputs=[{"output_type": "stream", "name": "stdout", "text": ["my work\n"]}])
            scratch = builder.cell("code", "my_notes = 'keep me'\n", "custom-scratch")
            content["cells"].append(scratch)
            content["cells"].insert(1, builder.cell("code", builder.LEGACY_BOOTSTRAP, "environment"))
            sample = next(c for c in content["cells"] if c.get("id") == "sample-data")
            sample["source"] = ['exercise_id = "37"\n', 'inputs, expected_output = show_sample(exercise_id, spark, globals().get("display"))\n',
                                'input_df = inputs["input_df"]\n', 'expected_df = expected_output\n']
            path.write_text(json.dumps(content), encoding="utf-8")
            updated, skipped = builder.upgrade_layout(folder)
            self.assertEqual((updated, skipped), ([path.name], []))
            after = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(next(c for c in after["cells"] if c.get("id") == "your-solution"), solution)
            self.assertEqual(after["cells"][-1], scratch)
            self.assertFalse(any(c.get("id") == "environment" for c in after["cells"]))
            self.assertEqual(builder.upgrade_layout(folder), ([], []))

    def test_customized_bootstrap_is_not_overwritten(self):
        spec = importlib.util.spec_from_file_location("builder", engine.ROOT / "tools/build_practice_notebooks.py")
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "37_latest_order.ipynb"
            content = builder.notebook([builder.cell("code", "my_custom_setup()\n", "environment")])
            path.write_text(json.dumps(content), encoding="utf-8")
            before = path.read_bytes()
            self.assertEqual(builder.upgrade_layout(folder), ([], [path.name]))
            self.assertEqual(path.read_bytes(), before)


@unittest.skipUnless(os.environ.get("SPARK_INTEGRATION") == "1", "Set SPARK_INTEGRATION=1 to run Spark tests")
class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("PracticeTests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_latest_order_end_to_end_and_saved_progress(self):
        from pyspark.sql import functions as F, Window
        def solve(input_df):
            window = Window.partitionBy("customer_id").orderBy(F.desc("order_date"), F.desc("order_id"))
            return input_df.withColumn("_rank", F.row_number().over(window)).filter("_rank=1").drop("_rank")
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {"DE_PRACTICE_PROGRESS_DIR": folder}):
            result = engine.check_solution("37", solve, self.spark)
            self.assertTrue(result["passed"])
            self.assertTrue(result["progress_saved"])
            row = progress.progress_rows(directory=folder)[36]
            self.assertEqual((row["status"], row["checks"]), ("Passed", 1))
            # A plausible but incorrect answer must fail the suite.
            result = engine.check_solution("37", lambda input_df: input_df, self.spark)
            self.assertFalse(result["passed"])
            self.assertEqual(progress.progress_rows(directory=folder)[36]["checks"], 2)

    def test_schema_duplicates_nested_order_and_numeric_tolerance(self):
        from pyspark.sql import functions as F
        ex = engine.load_exercise("37")["cases"][0]["expected"]
        expected = engine.expected_value(self.spark, ex)
        self.assertFalse(engine.compare_output(expected.select(*reversed(expected.columns)), expected, ex))
        self.assertTrue(engine.compare_output(expected.unionByName(expected.limit(1)), expected, ex))
        self.assertTrue(engine.compare_output(expected.withColumn("amount", F.col("amount").cast("string")), expected, ex))
        self.assertFalse(engine.compare_output(expected.withColumn("amount", F.col("amount").cast("int")), expected, ex))
        nested = engine.load_exercise("36")["cases"][0]["expected"]
        frame = engine.expected_value(self.spark, nested)
        self.assertFalse(engine.compare_output(frame.withColumn("products", F.reverse("products")), frame, nested))
        numeric = engine.load_exercise("39")["cases"][0]["expected"]
        frame = engine.expected_value(self.spark, numeric)
        self.assertFalse(engine.compare_output(frame.withColumn("running_total", F.col("running_total") + 1e-10), frame, numeric))

    def test_python_task_and_unimplemented_stub(self):
        def solve(l1, l2):
            appended, extended = l1.copy(), l1.copy()
            appended.append(l2)
            extended.extend(l2)
            return {"appended": appended, "extended": extended}
        self.assertTrue(engine.check_solution("15", solve, self.spark, record=False)["passed"])
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {"DE_PRACTICE_PROGRESS_DIR": folder}):
            result = engine.check_solution("37", lambda input_df: None, self.spark)
            self.assertTrue(result["unimplemented"])
            self.assertEqual(progress.read_events(), [])

    def test_multi_output_and_sample_notebook_execution(self):
        # Exercise the actual generated notebook cells without implementing a solution.
        notebook = json.loads((engine.ROOT / "practice/35_student_data_quality.ipynb").read_text(encoding="utf-8"))
        namespace = {"spark": self.spark}
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                execute_cell("".join(cell["source"]), namespace)
        self.assertNotIn("F", namespace)
        self.assertNotIn("Window", namespace)
        self.assertTrue(namespace["check_report"]["unimplemented"])
        spec = engine.load_exercise("35")["cases"][0]["expected"]
        expected = engine.expected_value(self.spark, spec)
        self.assertFalse(engine.compare_output(expected, expected, spec))
        self.assertTrue(engine.compare_output({"adults": expected["adults"]}, expected, spec))

    def test_shared_setup_single_input_python_and_dashboard(self):
        from practice_tools.notebook_setup import prepare
        for ident in ("1", "15", "dashboard"):
            namespace = {"spark": self.spark, "display": lambda value: None}
            prepare(ident, namespace)
            self.assertNotIn("F", namespace)
            self.assertNotIn("Window", namespace)
            if ident == "1":
                self.assertIn("input_df", namespace)
                self.assertIn("expected_df", namespace)
            elif ident == "15":
                self.assertEqual(namespace["l1"], [2, 3, 4, 5])
                self.assertNotIn("expected_df", namespace)
        dashboard = json.loads((engine.ROOT / "practice/00_Progress.ipynb").read_text(encoding="utf-8"))
        namespace = {"spark": self.spark, "display": lambda value: None}
        for cell in dashboard["cells"]:
            if cell["cell_type"] == "code":
                execute_cell("".join(cell["source"]), namespace)
        self.assertEqual(namespace["view_df"].count(), 43)
        self.assertNotIn("F", namespace)
        self.assertNotIn("Window", namespace)

    def test_nested_structures_materialize_and_field_order_is_relaxed(self):
        from pyspark.sql import functions as F
        for ident in ("19", "20"):
            case = engine.load_exercise(ident)["cases"][0]
            inputs = engine.case_inputs(self.spark, case)
            self.assertGreater(inputs["input_df"].count(), 0)
            expected = engine.expected_value(self.spark, case["expected"])
            actual = expected.select(*reversed(expected.columns))
            if ident == "20":
                actual = actual.withColumn("likeDislike", F.struct(
                    F.col("likeDislike.userAction"), F.col("likeDislike.likes"), F.col("likeDislike.dislikes")))
            self.assertFalse(engine.compare_output(actual, expected, case["expected"]))


if __name__ == "__main__":
    unittest.main()
