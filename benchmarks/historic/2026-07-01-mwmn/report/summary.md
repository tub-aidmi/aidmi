# Benchmark summary

## missing_relations_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| plan_write_critique | nvidia/mistral-medium-3.5-128b | 1 | 1 | 1 | 0.5563 | 1 | 1 | 3610 | 97 | 1.294e+06 | 3.566e+04 | 0 | 1 | 0 | 0 |
| write_then_critique | nvidia/mistral-medium-3.5-128b | 1 | - | - | - | - | 0 | 124.6 | - | - | - | - | - | - | - |
| write_tools_freeform | nvidia/mistral-medium-3.5-128b | 1 | 1 | 1 | 0.585 | 1 | 1 | 1258 | 40 | 7.377e+05 | 9514 | 0 | 1 | 0 | 0 |

## wrong_field_names_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| plan_write_critique | nvidia/mistral-medium-3.5-128b | 1 | 1 | 1 | 0.7824 | 1 | 1 | 2445 | 117 | 1.043e+06 | 9.325e+04 | 0 | 1 | 0 | 0 |
| write_then_critique | nvidia/mistral-medium-3.5-128b | 1 | 1 | 1 | 0.7988 | 1 | 1 | 811.6 | 55 | 2.597e+05 | 3.788e+04 | 0 | 1 | 0 | 0 |
| write_tools_freeform | nvidia/mistral-medium-3.5-128b | 1 | 1 | 1 | 0.7988 | 1 | 1 | 404.8 | 16 | 1.507e+05 | 5821 | 0 | 1 | 0 | 0 |

# Strategy × model — target_columns_covered (mean)

## missing_relations_v2

| strategy | nvidia/mistral-medium-3.5-128b |
|---|---|
| plan_write_critique | 1 |
| write_tools_freeform | 1 |

## wrong_field_names_v2

| strategy | nvidia/mistral-medium-3.5-128b |
|---|---|
| plan_write_critique | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |
