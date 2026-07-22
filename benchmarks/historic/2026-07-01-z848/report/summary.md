# Benchmark summary

## missing_relations_v2

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| write_then_critique | gemini-2.5-flash | 1 | 0 | 1 | 306.8 | 55 | 1.97e+05 | 7.728e+04 | 0.2419 | 0.8246 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 0 | 1 | 48.93 | 13 | 8.084e+04 | 6515 | 0.04054 | - | - | - |

## wrong_field_names_v2

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| write_then_critique | gemini-2.5-flash | 1 | 0 | 1 | 248.6 | 42 | 1.45e+05 | 4.557e+04 | 0.147 | 0.7719 | 0 | 0 |
| write_tools_freeform | gemini-2.5-flash | 1 | 0 | 1 | 86.21 | 13 | 9.092e+04 | 8195 | 0.02876 | - | - | - |

# Strategy × model — target_columns_covered (mean)

## missing_relations_v2

| strategy | gemini-2.5-flash |
|---|---|
| write_then_critique | 0.825 |

## wrong_field_names_v2

| strategy | gemini-2.5-flash |
|---|---|
| write_then_critique | 0.772 |
