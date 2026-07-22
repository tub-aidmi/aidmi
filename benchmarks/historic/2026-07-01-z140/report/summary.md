# Benchmark summary

## master

| spec | model | n | dbt_success | target_columns_covered | llm_calls_total | tokens_output_total | dollar_cost_total |
|---|---|---|---|---|---|---|---|
| freeform_ise_academicfix | ise-ollama/qwen3.6:35b-a3b | 1 | 0 | - | 5 | 3902 | 0 |
| structured_ise_academicfix | ise-ollama/qwen3.6:35b-a3b | 1 | 1 | 1 | 47 | 2.74e+04 | 0 |
| structured_ise_bigwriter | ise-ollama/gpt-oss:120b | 1 | - | - | - | - | - |
| structured_ise_selfheal | ise-ollama/qwen3.6:35b-a3b | 1 | 1 | 1 | 61 | 4.221e+04 | 0 |

## messy_data

| spec | model | n | dbt_success | target_columns_covered | llm_calls_total | tokens_output_total | dollar_cost_total |
|---|---|---|---|---|---|---|---|
| freeform_ise_academicfix | ise-ollama/qwen3.6:35b-a3b | 1 | 0 | - | 22 | 3178 | 0 |
| structured_ise_academicfix | ise-ollama/qwen3.6:35b-a3b | 1 | 0 | 0.5439 | 61 | 2.861e+04 | 0 |
| structured_ise_bigwriter | ise-ollama/gpt-oss:120b | 1 | - | - | - | - | - |
| structured_ise_selfheal | ise-ollama/qwen3.6:35b-a3b | 1 | 1 | 1 | 89 | 4.231e+04 | 0 |

# Strategy × model — target_columns_covered (mean)

## master

| strategy | ise-ollama/qwen3.6:35b-a3b |
|---|---|
| structured_per_table | 1 |

## messy_data

| strategy | ise-ollama/qwen3.6:35b-a3b |
|---|---|
| structured_per_table | 0.772 |
