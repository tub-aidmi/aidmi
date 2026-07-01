# Benchmark summary

## missing_relations_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| plan_write_critique | gemini-2.5-flash | 1 | 0 | 0.7017 | 0.6374 | 0.6 | 1 | 176.4 | 24 | 9.26e+04 | 3.946e+04 | 0.1214 | 0.5965 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 0 | 0 | - | 0 | 1 | 159.1 | 28 | 1.1e+05 | 3.57e+04 | 0.1192 | 0 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | - | - | - | - | 0 | 21.55 | - | - | - | - | - | - | - |

## wrong_field_names_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| plan_write_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.7987 | 1 | 1 | 136.5 | 18 | 2.592e+05 | 2.261e+04 | 0.09342 | 1 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | - | - | - | - | 0 | 42.37 | - | - | - | - | - | - | - |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 0.7988 | 1 | 1 | 139.6 | 8 | 5.073e+04 | 5620 | 0.02066 | 1 | 0 | 0 |

# Strategy × model — target_columns_covered (mean)

## missing_relations_v2

| strategy | gemini-2.5-flash |
|---|---|
| plan_write_critique | 0.596 |
| write_then_critique | 0 |

## wrong_field_names_v2

| strategy | gemini-2.5-flash |
|---|---|
| plan_write_critique | 1 |
| write_tools_freeform | 1 |
