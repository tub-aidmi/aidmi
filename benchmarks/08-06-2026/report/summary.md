# Benchmark summary

## sf_pipedrive_snapshot

| spec | model | n | ran_ok | dbt_success | target_columns_covered | type_mismatches | extraneous_columns | preservation_row_ratio_mean | preservation_empty_tables | preservation_null_inflation_mean | preservation_distinct_ratio_mean | manifest_present | manifest_table_coverage | manifest_column_coverage | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | wall_clock_seconds |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 3 | 1.84e+04±96 | 1.36e+03±1.6e+02 | 0 | 55.7±3.4 |
| critique_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 3.67±1.2 | 2.23e+04±7.2e+03 | 1.26e+04±4.6e+03 | 0 | 904±8.5e+02 |
| critique_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.225±0.23 | 0.981±0.019 | 1 | 1 | 1 | 4.33±1.2 | 2.64e+04±7.2e+03 | 7.99e+03±2.3e+03 | 0 | 161±83 |
| critique_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0 | 0 | 0 | 0 | - | 0 | - | - | 1 | 1 | 1 | 4.67±1.5 | 2.85e+04±9.4e+03 | 9.21e+03±5.6e+03 | 0 | 147±72 |
| ensemble_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 1.33±1.2 | 0 | 0.5 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 8 | 4.79e+04±1.7e+02 | 3.58e+03±1.6e+02 | 0 | 57.1±3.6 |
| ensemble_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 1.33±1.2 | 0 | 0.5 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 8 | 4.72e+04±1.1e+02 | 3.19e+04±2.7e+03 | 0 | 384±65 |
| ensemble_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 8 | 4.72e+04±62 | 9.84e+03±3.2e+02 | 0 | 110±9.3 |
| ensemble_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 0 | - | - | - | - | - | - | - | - | - | - | - | - | - | - | - | 186±17 |
| freeform_context_mode_live_query_tool | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | - | - | 0 | 0 | 0 | 6±1.7 | 2.06e+04±7.2e+03 | 2.72e+03±7e+02 | 0 | 173±49 |
| freeform_context_mode_metadata_only | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 1.33±1.2 | 0 | 0.5 | 0 | - | - | 0 | 0 | 0 | 5.33±4 | 1.98e+04±1.7e+04 | 3.64e+03±1.5e+03 | 0 | 323±2.3e+02 |
| freeform_context_mode_metadata_plus_samples | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | - | - | 0 | 0 | 0 | 3 | 2e+04±9.7e+02 | 2e+03±4e+02 | 0 | 99.8±4.4 |
| freeform_enable_self_correction_false | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | - | - | 0 | 0 | 0 | 4±1.7 | 2.81e+04±1.3e+04 | 2.38e+03±7e+02 | 0 | 171±47 |
| freeform_enable_self_correction_true | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | - | - | 0 | 0 | 0 | 5±2.6 | 3.5e+04±2e+04 | 1.88e+03±4.9e+02 | 0 | 151±30 |
| freeform_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | - | - | 0 | 0 | 0 | 6 | 4.07e+04±5.1e+02 | 1.02e+03±2.4e+02 | 0 | 128±6.7 |
| freeform_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.667±0.58 | 0.861±0.24 | 1.67±0.58 | 0 | 0.646±0.25 | 0 | - | - | 0 | 0 | 0 | 8.33±8.4 | 3.86e+04±2.2e+04 | 3.29e+03±1.3e+03 | 0 | 517±6e+02 |
| freeform_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | - | - | 0 | 0 | 0 | 6.33±0.58 | 4.54e+04±4.1e+03 | 1.77e+03±1.2e+02 | 0 | 215±10 |
| freeform_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 1.33±1.2 | 0 | 0.498±0.0033 | 0 | - | - | 0 | 0 | 0 | 15±13 | 9.59e+04±7.8e+04 | 2.58e+03±1.6e+03 | 0 | 247±1.9e+02 |
| plan_planner_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.15±0.26 | 0.987±0.022 | 1 | 1 | 1 | 3 | 1.81e+04±13 | 1.93e+03±69 | 0 | 73.2±7.4 |
| plan_planner_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0 | 0.194±0.34 | 0.333±0.58 | 0 | 0.939 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 3 | 1.81e+04±62 | 1.05e+04±2.9e+03 | 0 | 928±5.5e+02 |
| plan_planner_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.328±0.089 | 0.85±0.11 | 1 | 1 | 1 | 3 | 1.81e+04±5.1e+02 | 5.31e+03±2.1e+03 | 0 | 104±8.9 |
| plan_planner_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0 | 0.139±0.24 | 0.333±0.58 | 0 | 0.06103 | 0 | - | - | 1 | 1 | 1 | 3 | 1.78e+04±1.2e+02 | 3.76e+03±5e+02 | 0 | 72.5±6.5 |
| structured_context_mode_live_query_tool | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.333±0.58 | 0.528±0.5 | 1±1 | 0 | 0.719±0.31 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 2 | 4.22e+03±4.6 | 8e+03±5.4e+02 | 0 | 346±1.6e+02 |
| structured_context_mode_metadata_only | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 1.33±1.2 | 0 | 0.5 | 0 | 0.19±0.27 | 0.892±0.15 | 1 | 1 | 1 | 2 | 4.06e+03±6.9 | 9.35e+03±1.9e+03 | 0 | 834±2.8e+02 |
| structured_context_mode_metadata_plus_samples | ise-ollama/qwen3.6:35b-a3b | 3 | 0.333±0.58 | 0 | 0.5833 | 1 | 0 | 0.939 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 2 | 1.165e+04 | 5372 | 0 | 838±5.6e+02 |
| structured_enable_self_correction_false | ise-ollama/qwen3.6:35b-a3b | 3 | 0.333±0.58 | 0 | 0 | 0 | 0 | - | 0 | - | - | 1 | 1 | 1 | 2 | 1.165e+04 | 8532 | 0 | 890±4.6e+02 |
| structured_enable_self_correction_true | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.333±0.58 | 0.667±0.3 | 1.33±0.58 | 0 | 0.5±0.44 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 2 | 1.17e+04±13 | 8.63e+03±6.8e+02 | 0 | 253±17 |
| structured_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 2 | 1.18e+04±14 | 1.13e+03±6.6 | 0 | 49.7±4.8 |
| structured_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.333±0.58 | 0.528±0.5 | 1±1 | 0 | 0.719±0.31 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 2 | 1.17e+04±10 | 7e+03±1.9e+03 | 0 | 398±1.1e+02 |
| structured_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 3±1 | 0 | 0.5 | 0 | 0.277±0.089 | 0.915±0.11 | 1 | 1 | 1 | 2 | 1.17e+04±8.1 | 2.54e+03±6.7e+02 | 0 | 64±11 |
| structured_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0 | 0 | 0 | 0 | - | 0 | - | - | 1 | 1 | 0.806±0.34 | 2 | 1.17e+04±2.5 | 2.92e+03±6.8e+02 | 0 | 77.2±18 |
| structured_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 2 | 0 | 0.5 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 2 | 1.18e+04±8.3 | 1.25e+03±24 | 0 | 46.8±7.3 |
| structured_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.667±0.58 | 0.861±0.24 | 1.67±0.58 | 0 | 0.646±0.25 | 0 | 0.38 | 0.7846 | 1 | 1 | 1 | 2 | 1.17e+04±9.5 | 5.38e+03±2.1e+03 | 0 | 556±4.2e+02 |
| structured_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 0.667±0.58 | 0.806±0.34 | 2±1 | 0 | 0.354±0.25 | 0 | 0.45 | 0.9615 | 1 | 1 | 1 | 2 | 1.17e+04±9 | 2.8e+03±3.5e+02 | 0 | 64.1±22 |
| structured_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0 | 0 | 0 | 0 | - | 0 | - | - | 1 | 1 | 0.861±0.24 | 2 | 1.17e+04±6.7 | 2.6e+03±5.1e+02 | 0 | 44.9±3.8 |

## sp1_users

| spec | model | n | ran_ok | dbt_success | target_columns_covered | type_mismatches | extraneous_columns | row_count_match | preservation_row_ratio_mean | preservation_empty_tables | preservation_null_inflation_mean | preservation_distinct_ratio_mean | manifest_present | manifest_table_coverage | manifest_column_coverage | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | wall_clock_seconds |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | -0.00595±0.01 | 0.96±0.014 | 1 | 1 | 1 | 4 | 5.54e+03±38 | 1.39e+03±31 | 0 | 84.9±0.96 |
| critique_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 2 | 2.9e+03±27 | 4.69e+03±8.2e+02 | 0 | 151±26 |
| critique_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 4 | 6.09e+03±33 | 4.76e+03±8.6e+02 | 0 | 152±27 |
| critique_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0.333±0.58 | 0.333±0.58 | 0.333±0.58 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 4±1.7 | 6.15e+03±2.8e+03 | 9.38e+03±8.4e+03 | 0 | 178±1.3e+02 |
| ensemble_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 4 | 5.11e+03±15 | 1.71e+03±31 | 0 | 52.4±2.5 |
| ensemble_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 0.667±0.58 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 4 | 5.58e+03±79 | 7.2e+03±2e+03 | 0 | 238±10 |
| ensemble_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 4 | 5.7e+03±17 | 4.44e+03±2.2e+02 | 0 | 84.2±13 |
| ensemble_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 0.667±0.58 | 0 | 1 | 1 | 0 | 0 | 0.917±0.051 | 1 | 1 | 1 | 4 | 5.76e+03±33 | 4.31e+03±4.2e+02 | 0 | 65.3±4.9 |
| freeform_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 0.667±0.58 | 0 | 0 | 0 | 0 | - | - | 0 | - | - | 0 | 0 | 0 | 38±1.4 | 6.69e+04±3.6e+03 | 2.35e+03±4.2e+02 | 0 | 553±36 |
| freeform_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | - | - | 0 | 0 | 0 | 3.67±0.58 | 9.19e+03±2.4e+03 | 1.77e+03±3.7e+02 | 0 | 264±69 |
| freeform_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | - | - | 0 | 0 | 0 | 5.33±0.58 | 1.38e+04±1.6e+03 | 1.51e+03±1.3e+02 | 0 | 137±29 |
| freeform_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 0.333±0.58 | 0 | 0 | 0 | 0 | - | - | 0 | - | - | 0 | 0 | 0 | 69 | 2.21e+05 | 1.488e+04 | 0 | 883±3.6e+02 |
| mock_control | - | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | - | - | - | - | 15.5±0.15 |
| plan_planner_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 2 | 2.57e+03±41 | 968±46 | 0 | 55.6±1.8 |
| plan_planner_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 2 | 2.99e+03±96 | 3.68e+03±1.1e+03 | 0 | 842±5.7e+02 |
| plan_planner_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 2 | 2.81e+03±51 | 1.78e+03±2.2e+02 | 0 | 60±4.7 |
| plan_planner_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 0.667±0.58 | 0 | 1 | 1 | 0 | 0 | 0.929±0.034 | 1 | 1 | 1 | 2 | 2.81e+03±34 | 2.58e+03±1.1e+03 | 0 | 67.2±16 |
| structured_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 1 | 1.21e+03±2.3 | 543±6.1 | 0 | 40.2±0.22 |
| structured_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 1 | 1.37e+03±3.6 | 3.26e+03±3.2e+02 | 0 | 298±2.8e+02 |
| structured_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 1 | 1.38e+03±0.58 | 1.16e+03±1e+02 | 0 | 67.6±18 |
| structured_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0.667±0.58 | 0.667±0.58 | 0.667±0.58 | 0 | 1 | 1 | 0 | -0.0268±0.038 | 1.01±0.079 | 1 | 1 | 0.667±0.58 | 1 | 1.38e+03±3.5 | 1.08e+03±49 | 0 | 46.7±12 |
| structured_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 1 | 1.21e+03±5.6 | 552±5.2 | 0 | 51.2±13 |
| structured_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 1 | 1.37e+03±3.6 | 1.97e+03±9.3e+02 | 0 | 98.2±39 |
| structured_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | 0 | 0.9524 | 1 | 1 | 1 | 1 | 1.38e+03±1.5 | 1.1e+03±1.5e+02 | 0 | 44.7±17 |
| structured_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 0 | -0.0119±0.021 | 0.983±0.027 | 1 | 1 | 0.714±0.49 | 1 | 1.38e+03±3.8 | 1.26e+03±1.2e+02 | 0 | 550±12 |

# Strategy × model — target_columns_covered (mean)

## sf_pipedrive_snapshot

| strategy | academic/devstral-2-123b-instruct-2512 | academic/qwen3.5-397b-a17b | ise-ollama/qwen3.6:35b-a3b | ise-openai-nvidia/qwen35-9b |
|---|---|---|---|---|
| ensemble_vote | 0.667 | 1 | 0.667 | - |
| plan_then_execute | 1 | 1 | 0.194 | 0.139 |
| structured_per_table | 1 | 0.903 | 0.548 | 0 |
| write_then_critique | 1 | 1 | 1 | 0 |
| write_tools_freeform | 1 | 1 | 0.921 | 0.667 |

## sp1_users

| strategy | - | academic/devstral-2-123b-instruct-2512 | academic/qwen3.5-397b-a17b | ise-ollama/qwen3.6:35b-a3b | ise-openai-nvidia/qwen35-9b |
|---|---|---|---|---|---|
| ensemble_vote | - | 1 | 1 | 0.667 | 0.667 |
| mock | 1 | - | - | - | - |
| plan_then_execute | - | 1 | 1 | 1 | 0.667 |
| structured_per_table | - | 1 | 1 | 1 | 0.833 |
| write_then_critique | - | 1 | 1 | 1 | 0.333 |
| write_tools_freeform | - | 0 | 1 | 1 | 0 |
