# Exercise contracts and fixtures

Each numbered JSON file contains the question, topic, difficulty, typed input
data, expected output data, and additional test cases. These are fixture data,
not executable solution code. `practice_tools` loads them using paths relative
to its own module; reference notebooks are never imported.

Each case has named `inputs` and `expected`. A `dataframe` value contains a Spark
StructType JSON schema plus rows. `python` contains a native value. `outputs`
contains several named DataFrame specifications. `unordered_arrays` lists the
specific output array columns for which ordering does not matter. Other arrays
are checked in order. The first case is displayed in the practice notebook.

## Clarifications compared with the original examples

| Scenario | Practice contract |
| --- | --- |
| 1 | A worker is returned once, even if more than two workers share the salary. |
| 2 | All dispatched records with an Ordered record; not first transitions only. |
| 3 | Sort sensor rows by reading value; retain the current timestamp. |
| 4 | Output distinct address arrays, not counts; array order is ungraded. |
| 5 | Check union/email transformation; partition observations and writes are optional discussion. |
| 6 | Correct spelling: Designation. |
| 9 | Return a name DataFrame, include tied winners, exclude zero-first-place participants. |
| 10 | Parse calendar dates: December is later than October. |
| 12 | Explicit positional masking and null preservation. |
| 15 | Independent copies for append/extend, returned as a Python dictionary. |
| 16 | Deduplicate names by smallest id. |
| 17 | Full outer merge; preserve unmatched records. |
| 19 | Media fields replace colliding parent fields, matching the original convention. |
| 20 | Nested output graded with field-order/nullability relaxed. |
| 22 | Cumulative totals ordered by calendar date, not price. |
| 23 | Derive qualifying customers; include duplicate-purchase and empty-catalog cases. |
| 24 | Added event_seq makes ordered page arrays meaningful after a shuffle. |
| 25 | Raw CSV includes headers/quoted commas; check output, not a particular read mode. |
| 26 | Null-safe names; determine missing records by ID. |
| 29 | Set union excluding max of first set, including empty-first-set behavior. |
| 30, 38 | Second distinct salary, including ties; omit single-salary departments. |
| 31 | Four unpivoted values; no stray extra blank row. |
| 33 | Return winning family names and counts, including ties. |
| 34 | Age 51 belongs to 51+. |
| 35 | Check null counts, quarantined originals, imputed adults; explicit half-up rounding/all-null rule. |
| 36 | Original name null_sell means distinct-product count. |
| 37 | Highest order_id breaks equal-date ties. |
| 39 | Running balance; original salary heading was a copy/paste error. |
| 40 | Distinct calendar days; one output per qualifying user. |
| 41 | Customer input; deterministic timestamp ties. |
| 42 | Correct spelling: occurrence_count. |

Samples originate in the upstream scenarios. Expected results and additional
cases follow these clarified contracts. Original reference files are retained
for study, including any differences from these contracts.
