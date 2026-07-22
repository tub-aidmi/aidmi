# Benchmark summary

## master_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 0 | 0.7611 | 0.7415 | 0.8 | 1 | 337.2 | 57 | 2.118e+05 | 1.308e+05 | 0.3769 | 0.807 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 1 | 1 | 0.6158 | 1 | 1 | 98.96 | 6 | 1.975e+04 | 2.464e+04 | 0.06752 | 1 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 0 | 0.7611 | 0.6678 | 0.8 | 1 | 571.7 | 29 | 1.377e+05 | 7.227e+04 | 0.2095 | 0.807 | 0 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 1 | 1 | 0.7357 | 1 | 1 | 269.5 | 35 | 1.423e+05 | 5.596e+04 | 0.1667 | 1 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.7162 | 1 | 1 | 194.5 | 28 | 1.186e+05 | 5.663e+04 | 0.1677 | 1 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 0.7375 | 1 | 1 | 99.21 | 25 | 2.026e+05 | 1.312e+04 | 0.08366 | 1 | 0 | 0 |

## messy_data_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 1 | 1 | 0.7107 | 1 | 1 | 124.6 | 45 | 1.585e+05 | 6.107e+04 | 0.1869 | 1 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 1 | 1 | 0.6816 | 1 | 1 | 116.4 | 14 | 5.547e+04 | 2.841e+04 | 0.08547 | 1 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.6848 | 1 | 1 | 315.8 | 54 | 6.418e+05 | 5.745e+04 | 0.2914 | 1 | 0 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 1 | 1 | 0.7223 | 1 | 1 | 59.07 | 14 | 5.229e+04 | 1.656e+04 | 0.0515 | 1 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.7402 | 1 | 1 | 111.6 | 22 | 8.538e+04 | 3.199e+04 | 0.1008 | 1 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 0.7331 | 1 | 1 | 98.1 | 24 | 1.817e+05 | 1.551e+04 | 0.05461 | 1 | 0 | 0 |

## missing_relations_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 1 | 1 | 0.6019 | 1 | 1 | 76.76 | 38 | 1.27e+05 | 5.745e+04 | 0.1691 | 1 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 1 | 1 | 0.5296 | 1 | 1 | 56.19 | 8 | 2.902e+04 | 1.627e+04 | 0.04747 | 1 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.596 | 1 | 1 | 245.6 | 46 | 4.726e+05 | 5.553e+04 | 0.2355 | 1 | 0 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 1 | 1 | 0.5776 | 1 | 1 | 51.74 | 13 | 4.592e+04 | 1.583e+04 | 0.05134 | 1 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.6019 | 1 | 1 | 121.9 | 33 | 1.305e+05 | 4.128e+04 | 0.1312 | 1 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 0.5902 | 1 | 1 | 49.46 | 13 | 7.817e+04 | 6490 | 0.02371 | 1 | 0 | 0 |

## wrong_field_names_v2

| spec | model | n | dbt_success | gt_recall_overall | gt_field_accuracy_overall | gt_tables_materialized | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 1 | 1 | 0.7988 | 1 | 1 | 53.83 | 41 | 1.391e+05 | 4.564e+04 | 0.1499 | 1 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 1 | 1 | 0.6672 | 1 | 1 | 84.39 | 8 | 2.836e+04 | 2.337e+04 | 0.06694 | 1 | 4 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.7724 | 1 | 1 | 423.7 | 89 | 9.003e+05 | 6.99e+04 | 0.2505 | 1 | 0 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 1 | 1 | 0.7988 | 1 | 1 | 31.54 | 11 | 3.838e+04 | 1.18e+04 | 0.03802 | 1 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 1 | 1 | 0.7786 | 1 | 1 | 84.41 | 24 | 8.829e+04 | 3.22e+04 | 0.09894 | 1 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 0.7988 | 1 | 1 | 47.28 | 13 | 9.443e+04 | 5530 | 0.04215 | 1 | 0 | 0 |

# Strategy × model — target_columns_covered (mean)

## master_v2

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 0.807 |
| plan_then_execute | 1 |
| plan_write_critique | 0.807 |
| structured_per_table | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |

## messy_data_v2

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 1 |
| plan_then_execute | 1 |
| plan_write_critique | 1 |
| structured_per_table | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |

## missing_relations_v2

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 1 |
| plan_then_execute | 1 |
| plan_write_critique | 1 |
| structured_per_table | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |

## wrong_field_names_v2

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 1 |
| plan_then_execute | 1 |
| plan_write_critique | 1 |
| structured_per_table | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |
