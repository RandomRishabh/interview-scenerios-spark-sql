"""Shared notebook initialization, without importing learners' PySpark shortcuts."""

from .engine import show_sample, check_solution
from .progress import mark_attempted, progress_dataframe


def show_progress(exercise_id, spark):
    frame = progress_dataframe(spark)
    frame.filter(frame["id"] == exercise_id).show(truncate=False)


def prepare(exercise_id, namespace):
    """Populate exercise variables and display fixtures; leave user imports alone."""
    if "spark" not in namespace:
        from pyspark.sql import SparkSession
        namespace["spark"] = (SparkSession.builder.master("local[2]")
                              .appName("InterviewPractice").getOrCreate())
    namespace.update(check_solution=check_solution, mark_attempted=mark_attempted,
                     progress_dataframe=progress_dataframe, show_progress=show_progress)
    if exercise_id == "dashboard":
        return
    inputs, expected = show_sample(exercise_id, namespace["spark"], namespace.get("display"))
    namespace.update(exercise_id=exercise_id, inputs=inputs, expected_output=expected)
    namespace.update(inputs)
    if hasattr(expected, "schema"):
        namespace["expected_df"] = expected
    elif isinstance(expected, dict):
        for name, value in expected.items():
            if hasattr(value, "schema"):
                namespace[f"expected_{name}_df"] = value
