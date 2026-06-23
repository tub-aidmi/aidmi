# Benchmark summary

## sf_pipedrive_snapshot

| spec | model | n | preservation_row_ratio_mean | preservation_null_inflation_mean | preservation_distinct_ratio_mean | preservation_empty_tables | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | manifest_present | manifest_table_coverage | manifest_column_coverage | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_writer_model_gptoss120b | academic/openai-gpt-oss-120b | 1 | - | - | - | - | - | 0 | 28.62 | - | - | - | - | - | - | - | - | - | - |
| critique_writer_model_mistral128b | nvidia/mistral-medium-3.5-128b | 1 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 252.1 | 3 | 1.803e+04 | 2640 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| critique_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 1 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 804.9 | 8 | 4.964e+04 | 3.385e+04 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| ensemble_writer_model_gptoss120b | academic/openai-gpt-oss-120b | 1 | - | - | - | 0 | 0 | 1 | 73.26 | 14 | 7.864e+04 | 1.102e+04 | 0 | 1 | 1 | 1 | 0 | 0 | 0 |
| ensemble_writer_model_mistral128b | nvidia/mistral-medium-3.5-128b | 1 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 467.3 | 8 | 4.721e+04 | 6865 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| ensemble_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 1 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 326.8 | 8 | 4.705e+04 | 2.726e+04 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| freeform_writer_model_gptoss120b | academic/openai-gpt-oss-120b | 1 | 0.5 | - | - | 0 | 1 | 1 | 50.5 | 2 | 1.269e+04 | 1664 | 0 | 0 | 0 | 0 | 1 | 2 | 0 |
| freeform_writer_model_mistral128b | nvidia/mistral-medium-3.5-128b | 1 | 0.5 | - | - | 0 | 1 | 1 | 559.9 | 7 | 4.919e+04 | 1601 | 0 | 0 | 0 | 0 | 1 | 2 | 0 |
| freeform_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 1 | 0.5 | - | - | 0 | 1 | 1 | 120 | 5 | 3.57e+04 | 2244 | 0 | 0 | 0 | 0 | 1 | 2 | 0 |
| plan_planner_model_gptoss120b | academic/openai-gpt-oss-120b | 1 | - | - | - | - | - | 0 | 35.11 | - | - | - | - | - | - | - | - | - | - |
| plan_planner_model_mistral128b | nvidia/mistral-medium-3.5-128b | 1 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 367.3 | 3 | 1.836e+04 | 4727 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| plan_planner_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 1 | 0.4953 | 0.4495 | 0.9231 | 0 | 1 | 1 | 204 | 4 | 2.658e+04 | 9341 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| structured_writer_model_gptoss120b | academic/openai-gpt-oss-120b | 1 | 0.5 | - | - | 0 | 1 | 1 | 65.62 | 3 | 1.6e+04 | 2094 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| structured_writer_model_mistral128b | nvidia/mistral-medium-3.5-128b | 1 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 327.1 | 2 | 1.16e+04 | 1638 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| structured_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 1 | 0.939 | 0.38 | 0.7846 | 0 | 0 | 1 | 296.2 | 2 | 1.166e+04 | 1.099e+04 | 0 | 1 | 1 | 1 | 0.5833 | 1 | 0 |

# Strategy × model — target_columns_covered (mean)

## sf_pipedrive_snapshot

| strategy | academic/openai-gpt-oss-120b | ise-ollama/qwen3.6:35b-a3b | nvidia/mistral-medium-3.5-128b |
|---|---|---|---|
| ensemble_vote | 0 | 1 | 1 |
| plan_then_execute | - | 1 | 1 |
| structured_per_table | 1 | 0.583 | 1 |
| write_then_critique | - | 1 | 1 |
| write_tools_freeform | 1 | 1 | 1 |
