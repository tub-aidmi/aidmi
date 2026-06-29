# Benchmark summary

## master

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 0 | 1 | 300.7 | 82 | 2.801e+05 | 1.571e+05 | 0.4536 | 0.807 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 0 | 1 | 302.1 | 40 | 1.355e+05 | 9.604e+04 | 0.2702 | 0.8246 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 0 | 1 | 704.7 | 69 | 4.432e+05 | 1.389e+05 | 0.4224 | 1 | 0 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 0 | 1 | 303.3 | 44 | 1.594e+05 | 1.099e+05 | 0.3123 | 0.6316 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 1 | 1 | 406.9 | 36 | 1.705e+05 | 1.248e+05 | 0.3507 | 1 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 0 | 1 | 185.5 | 35 | 1.703e+05 | 2.108e+04 | 0.06986 | 0.6316 | 0 | 0 |

## messy_data

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 0 | 1 | 360.9 | 75 | 2.65e+05 | 1.414e+05 | 0.4159 | 0.807 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 0 | 1 | 355.3 | 36 | 1.305e+05 | 9.478e+04 | 0.2693 | 0.807 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 0 | 1 | 239.8 | 49 | 1.917e+05 | 9.452e+04 | 0.2801 | 0.4561 | 0 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 0 | 1 | 299.8 | 51 | 1.931e+05 | 9.652e+04 | 0.2824 | 0.807 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 0 | 1 | 359.4 | 52 | 1.811e+05 | 1.039e+05 | 0.3061 | 0.8246 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 68.07 | 9 | 4.066e+04 | 3667 | 0.0152 | 1 | 0 | 0 |

## missing_relations

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 1 | 1 | 210.7 | 46 | 1.368e+05 | 7.241e+04 | 0.2111 | 1 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 1 | 1 | 95.77 | 6 | 1.542e+04 | 2.013e+04 | 0.05428 | 1 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 1 | 1 | 164.2 | 19 | 1.485e+05 | 2.211e+04 | 0.07836 | 1 | 0 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 1 | 1 | 53.75 | 13 | 3.61e+04 | 1.472e+04 | 0.04376 | 1 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 1 | 1 | 348.4 | 48 | 1.527e+05 | 8.82e+04 | 0.2528 | 1 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 69.88 | 13 | 8.033e+04 | 6020 | 0.02974 | 1 | 0 | 0 |

## wrong_field_names

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ensemble_vote | gemini-2.5-flash | 1 | 1 | 1 | 77.63 | 42 | 1.225e+05 | 4.97e+04 | 0.1538 | 1 | 0 | 0 |
| plan_then_execute | gemini-2.5-flash | 1 | 1 | 1 | 185.4 | 12 | 4.221e+04 | 2.212e+04 | 0.06513 | 1 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 1 | 1 | 160.1 | 23 | 1.87e+05 | 2.212e+04 | 0.07694 | 1 | 1 | 0 |
| structured_per_table | gemini-2.5-flash | 1 | 1 | 1 | 50.92 | 18 | 6.01e+04 | 1.228e+04 | 0.04191 | 1 | 0 | 0 |
| write_then_critique | gemini-2.5-flash | 1 | 1 | 1 | 400 | 26 | 8.071e+04 | 3.618e+04 | 0.1087 | 1 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 1 | 1 | 66.15 | 12 | 6.64e+04 | 4777 | 0.01871 | 1 | 0 | 0 |

# Strategy × model — target_columns_covered (mean)

## master

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 0.807 |
| plan_then_execute | 0.825 |
| plan_write_critique | 1 |
| structured_per_table | 0.632 |
| write_then_critique | 1 |
| write_tools_freeform | 0.632 |

## messy_data

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 0.807 |
| plan_then_execute | 0.807 |
| plan_write_critique | 0.456 |
| structured_per_table | 0.807 |
| write_then_critique | 0.825 |
| write_tools_freeform | 1 |

## missing_relations

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 1 |
| plan_then_execute | 1 |
| plan_write_critique | 1 |
| structured_per_table | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |

## wrong_field_names

| strategy | gemini-2.5-flash |
|---|---|
| ensemble_vote | 1 |
| plan_then_execute | 1 |
| plan_write_critique | 1 |
| structured_per_table | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |
