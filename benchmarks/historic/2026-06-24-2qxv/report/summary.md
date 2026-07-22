# Benchmark summary

## master

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| freeform | gemini-2.5-flash | 1 | 1 | 1 | 162.7 | 28 | 1.771e+05 | 1.876e+04 | 0.06294 | 1 | 0 | 0 |
| plan_write_critique | gemini-2.5-flash | 1 | 0 | 1 | 420.7 | 41 | 2.24e+05 | 1.05e+05 | 0.4214 | 1 | 0 | 0 |

# Strategy × model — target_columns_covered (mean)

## master

| strategy | gemini-2.5-flash |
|---|---|
| plan_write_critique | 1 |
| write_tools_freeform | 1 |
