# Benchmark summary

## master

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_conservative | gemini-2.5-flash | 1 | 1 | 1 | 251.8 | 27 | 9.557e+04 | 3.066e+04 | 0.113 | 1 | 0 | 0 |
| freeform_inline_dbt | gemini-2.5-flash | 1 | 1 | 1 | 110 | 10 | 5.767e+04 | 5284 | 0.0181 | 1 | 0 | 0 |
| structured_selfcorrect | gemini-2.5-flash | 1 | 0 | 1 | 1074 | 81 | 3.495e+05 | 1.342e+05 | 0.3989 | 1 | 0 | 0 |

## messy_data

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_conservative | gemini-2.5-flash | 1 | 0 | 1 | 745.7 | 59 | 2.145e+05 | 8.834e+04 | 0.2891 | 1 | 0 | 0 |
| freeform_inline_dbt | gemini-2.5-flash | 1 | 0 | 1 | 307.5 | 28 | 4.921e+05 | 2.892e+04 | 0.1033 | - | - | - |
| structured_selfcorrect | gemini-2.5-flash | 1 | 0 | 1 | 1002 | 72 | 2.536e+05 | 1.217e+05 | 0.3584 | 0.8246 | 0 | 0 |

# Strategy × model — target_columns_covered (mean)

## master

| strategy | gemini-2.5-flash |
|---|---|
| structured_per_table | 1 |
| write_then_critique | 1 |
| write_tools_freeform | 1 |

## messy_data

| strategy | gemini-2.5-flash |
|---|---|
| structured_per_table | 0.825 |
| write_then_critique | 1 |
