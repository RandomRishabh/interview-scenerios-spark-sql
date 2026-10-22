"""Check small interview datasets without importing reference solutions."""

import copy
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAX_ROWS = 2000


def load_exercise(exercise_id):
    if not isinstance(exercise_id, str) or not exercise_id.isdigit():
        raise ValueError("Use a numeric scenario ID, for example '37'.")
    path = ROOT / "exercises" / f"{int(exercise_id):02d}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _frame(spark, spec):
    from pyspark.sql.types import StructType
    schema = StructType.fromJson(spec["schema"])
    # Tuples at the top level prevent dict/field order ambiguity. Nested structs
    # remain dictionaries, which createDataFrame maps by field name.
    rows = [tuple(row.get(f.name) for f in schema) if isinstance(row, dict)
            else tuple(row) for row in spec["rows"]]
    return spark.createDataFrame(rows, schema)


def case_inputs(spark, case):
    return {name: (_frame(spark, spec) if spec["kind"] == "dataframe"
                   else copy.deepcopy(spec["value"]))
            for name, spec in case["inputs"].items()}


def expected_value(spark, spec):
    if spec["kind"] == "dataframe":
        return _frame(spark, spec)
    if spec["kind"] == "outputs":
        return {name: expected_value(spark, value) for name, value in spec["values"].items()}
    return copy.deepcopy(spec["value"])


def show_sample(exercise_id, spark, display_fn=None):
    """Display fixture data and return fresh inputs plus expected output."""
    exercise = load_exercise(exercise_id)
    case = exercise["cases"][0]
    inputs = case_inputs(spark, case)
    expected = expected_value(spark, case["expected"])

    def show(label, value):
        print(label)
        if isinstance(value, dict):
            for name, part in value.items():
                show(name, part)
        elif hasattr(value, "schema") and hasattr(value, "show"):
            if display_fn:
                display_fn(value)
            else:
                value.show(truncate=False)
        else:
            print(value)

    for name, value in inputs.items():
        show("INPUT: " + name, value)
    show("EXPECTED OUTPUT", expected)
    return inputs, expected


def _schema_type(data_type):
    """Ignore nullable/metadata and integer widths; keep names and structure."""
    from pyspark.sql.types import ArrayType, StructType, MapType
    if isinstance(data_type, StructType):
        return ("struct", tuple(sorted((f.name, _schema_type(f.dataType)) for f in data_type)))
    if isinstance(data_type, ArrayType):
        return ("array", _schema_type(data_type.elementType))
    if isinstance(data_type, MapType):
        return ("map", _schema_type(data_type.keyType), _schema_type(data_type.valueType))
    name = data_type.typeName()
    return "integer" if name in {"byte", "short", "integer", "long"} else data_type.simpleString()


def _plain(value):
    if hasattr(value, "asDict"):
        return {k: _plain(v) for k, v in value.asDict().items()}
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _matching(left, right, equal):
    """Maximum bipartite matching preserves multiplicity, even with tolerance."""
    edges = [[j for j, b in enumerate(right) if equal(a, b)] for a in left]
    owners = {}

    def assign(i, seen):
        for j in edges[i]:
            if j in seen:
                continue
            seen.add(j)
            if j not in owners or assign(owners[j], seen):
                owners[j] = i
                return True
        return False

    for i in range(len(left)):
        assign(i, set())
    matched = set(owners.values())
    return [a for i, a in enumerate(left) if i not in matched], [b for j, b in enumerate(right) if j not in owners]


def _equal(a, b, unordered, path=""):
    if a is None or b is None:
        return a is b
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(
            _equal(a[k], b[k], unordered, f"{path}.{k}".lstrip(".")) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        if path in unordered:
            missing, extra = _matching(a, b, lambda x, y: _equal(x, y, unordered, path + "[]"))
            return not missing and not extra
        return all(_equal(x, y, unordered, path + "[]") for x, y in zip(a, b))
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)
    return type(a) is type(b) and a == b


def compare_output(actual, expected, spec, label="output"):
    """Return errors; top-level rows/columns are unordered, array order is explicit."""
    if spec["kind"] == "outputs":
        if not isinstance(actual, dict) or actual.keys() != expected.keys():
            return [f"Return a dictionary with outputs: {', '.join(expected)}."]
        return [error for name in expected for error in compare_output(
            actual[name], expected[name], spec["values"][name], name)]
    unordered = set(spec.get("unordered_arrays", []))
    if spec["kind"] == "python":
        return [] if _equal(_plain(actual), _plain(expected), unordered) else [
            f"{label}: expected {expected!r}; received {actual!r}."]
    if not (hasattr(actual, "schema") and hasattr(actual, "collect")):
        return [f"{label}: return a Spark DataFrame, not {type(actual).__name__}."]
    if len(actual.columns) != len(set(actual.columns)):
        return [f"{label}: duplicate column names; select/alias the required output columns."]
    if _schema_type(actual.schema) != _schema_type(expected.schema):
        return [f"{label}: incorrect columns/types. Expected {expected.schema.simpleString()}; "
                f"received {actual.schema.simpleString()}."]
    a = [_plain(row) for row in actual.limit(MAX_ROWS + 1).collect()]
    b = [_plain(row) for row in expected.collect()]
    if len(a) > MAX_ROWS:
        return [f"{label}: over {MAX_ROWS} rows; these fixtures are deliberately small."]
    # Linear-time exact match for the common case avoids quadratic comparison.
    if not unordered and not any('double' in f.dataType.simpleString() or
                                 'float' in f.dataType.simpleString() for f in expected.schema):
        from collections import Counter
        def key(row):
            return json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
        ac, bc = Counter(map(key, a)), Counter(map(key, b))
        missing, extra = list((bc-ac).elements()), list((ac-bc).elements())
    else:
        missing, extra = _matching(b, a, lambda x, y: _equal(x, y, unordered))
    if not missing and not extra:
        return []
    return [f"{label}: missing rows (up to 5): {missing[:5]}",
            f"{label}: unexpected rows (up to 5): {extra[:5]}"]


def check_solution(exercise_id, solve, spark, *, record=True):
    """Run every case on fresh inputs. No reference-solution code is loaded."""
    exercise = load_exercise(exercise_id)
    results = []
    unimplemented = False
    for index, case in enumerate(exercise["cases"]):
        try:
            actual = solve(**case_inputs(spark, case))
            if actual is None and index == 0:
                print("Your solution still returns None. Edit solve(...) first; no attempt recorded.")
                unimplemented = True
                break
            expected = expected_value(spark, case["expected"])
            errors = compare_output(actual, expected, case["expected"])
        except Exception as exc:
            errors = [f"{type(exc).__name__}: {exc}"]
        results.append({"case": case["name"], "passed": not errors, "errors": errors})
        print(("PASS" if not errors else "FAIL") + ": " + case["name"])
        for error in errors:
            print("  " + error)
    passed = bool(results) and not unimplemented and all(r["passed"] for r in results)
    saved = False
    if record and not unimplemented:
        from .progress import record_event
        try:
            record_event(exercise_id, "passed" if passed else "failed", spark=spark)
            saved = True
        except Exception as exc:
            print(f"PROGRESS NOT SAVED: {exc}. Your check result above is still valid.")
    if not unimplemented:
        print(f"{sum(r['passed'] for r in results)}/{len(results)} cases passed.")
    return {"passed": passed, "cases": results, "progress_saved": saved,
            "unimplemented": unimplemented}
