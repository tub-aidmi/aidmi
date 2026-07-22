# Benchmark summary

## master

| spec | model | n | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique | nvidia/mistral-medium-3.5-128b | 1 | 0 | 1 | 1214 | 13 | 4.896e+04 | 2.189e+04 | 0 | 0 | 0 | 0 |
| ensemble | nvidia/mistral-medium-3.5-128b | 1 | 0 | 1 | 118.1 | 20 | 6.371e+04 | 2.875e+04 | 0 | 0 | 0 | 0 |
| freeform | nvidia/mistral-medium-3.5-128b | 1 | 1 | 1 | 485.5 | 9 | 4.392e+04 | 2721 | 0 | 0 | 0 | 57 |
| plan | nvidia/mistral-medium-3.5-128b | 1 | 1 | 1 | 952.2 | 6 | 2.166e+04 | 1.27e+04 | 0 | 0 | 0 | 57 |
| structured | nvidia/mistral-medium-3.5-128b | 1 | 0 | 1 | 262.9 | 5 | 1.485e+04 | 9475 | 0 | 0 | 0 | 0 |

# Strategy × model — target_columns_covered (mean)

## master

| strategy | nvidia/mistral-medium-3.5-128b |
|---|---|
| ensemble_vote | 0 |
| plan_then_execute | 0 |
| structured_per_table | 0 |
| write_then_critique | 0 |
| write_tools_freeform | 0 |
