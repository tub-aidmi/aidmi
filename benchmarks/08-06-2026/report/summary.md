# Benchmark summary

## sf_pipedrive_snapshot

| spec | model | n | preservation_row_ratio_mean | preservation_null_inflation_mean | preservation_distinct_ratio_mean | preservation_empty_tables | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | manifest_present | manifest_table_coverage | manifest_column_coverage | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 55.7±3.4 | 3 | 1.84e+04±96 | 1.36e+03±1.6e+02 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| critique_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 904±8.5e+02 | 3.67±1.2 | 2.23e+04±7.2e+03 | 1.26e+04±4.6e+03 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| critique_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 0.5 | 0.225±0.23 | 0.981±0.019 | 0 | 1 | 1 | 161±83 | 4.33±1.2 | 2.64e+04±7.2e+03 | 7.99e+03±2.3e+03 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| critique_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | - | - | - | 0 | 0 | 1 | 147±72 | 4.67±1.5 | 2.85e+04±9.4e+03 | 9.21e+03±5.6e+03 | 0 | 1 | 1 | 1 | 0 | 0 | 0 |
| ensemble_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 0.5 | 0.38 | 0.7846 | 0 | 0.667±0.58 | 1 | 57.1±3.6 | 8 | 4.79e+04±1.7e+02 | 3.58e+03±1.6e+02 | 0 | 1 | 1 | 1 | 0.667±0.58 | 1.33±1.2 | 0 |
| ensemble_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 0.5 | 0.38 | 0.7846 | 0 | 0.667±0.58 | 1 | 384±65 | 8 | 4.72e+04±1.1e+02 | 3.19e+04±2.7e+03 | 0 | 1 | 1 | 1 | 0.667±0.58 | 1.33±1.2 | 0 |
| ensemble_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 110±9.3 | 8 | 4.72e+04±62 | 9.84e+03±3.2e+02 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| ensemble_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | - | - | - | - | - | 0 | 186±17 | - | - | - | - | - | - | - | - | - | - |
| freeform_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 0.5 | - | - | 0 | 1 | 1 | 128±6.7 | 6 | 4.07e+04±5.1e+02 | 1.02e+03±2.4e+02 | 0 | 0 | 0 | 0 | 1 | 2 | 0 |
| freeform_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 0.646±0.25 | - | - | 0 | 0.667±0.58 | 1 | 517±6e+02 | 8.33±8.4 | 3.86e+04±2.2e+04 | 3.29e+03±1.3e+03 | 0 | 0 | 0 | 0 | 0.861±0.24 | 1.67±0.58 | 0 |
| freeform_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 0.5 | - | - | 0 | 1 | 1 | 215±10 | 6.33±0.58 | 4.54e+04±4.1e+03 | 1.77e+03±1.2e+02 | 0 | 0 | 0 | 0 | 1 | 2 | 0 |
| freeform_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 0.498±0.0033 | - | - | 0 | 0.667±0.58 | 1 | 247±1.9e+02 | 15±13 | 9.59e+04±7.8e+04 | 2.58e+03±1.6e+03 | 0 | 0 | 0 | 0 | 0.667±0.58 | 1.33±1.2 | 0 |
| plan_planner_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 0.5 | 0.15±0.26 | 0.987±0.022 | 0 | 1 | 1 | 73.2±7.4 | 3 | 1.81e+04±13 | 1.93e+03±69 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| plan_planner_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 0.939 | 0.38 | 0.7846 | 0 | 0 | 1 | 928±5.5e+02 | 3 | 1.81e+04±62 | 1.05e+04±2.9e+03 | 0 | 1 | 1 | 1 | 0.194±0.34 | 0.333±0.58 | 0 |
| plan_planner_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 0.5 | 0.328±0.089 | 0.85±0.11 | 0 | 1 | 1 | 104±8.9 | 3 | 1.81e+04±5.1e+02 | 5.31e+03±2.1e+03 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| plan_planner_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 0.06103 | - | - | 0 | 0 | 1 | 72.5±6.5 | 3 | 1.78e+04±1.2e+02 | 3.76e+03±5e+02 | 0 | 1 | 1 | 1 | 0.139±0.24 | 0.333±0.58 | 0 |
| structured_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 49.7±4.8 | 2 | 1.18e+04±14 | 1.13e+03±6.6 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| structured_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 0.719±0.31 | 0.38 | 0.7846 | 0 | 0.333±0.58 | 1 | 398±1.1e+02 | 2 | 1.17e+04±10 | 7e+03±1.9e+03 | 0 | 1 | 1 | 1 | 0.528±0.5 | 1±1 | 0 |
| structured_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 0.5 | 0.277±0.089 | 0.915±0.11 | 0 | 1 | 1 | 64±11 | 2 | 1.17e+04±8.1 | 2.54e+03±6.7e+02 | 0 | 1 | 1 | 1 | 1 | 3±1 | 0 |
| structured_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | - | - | - | 0 | 0 | 1 | 77.2±18 | 2 | 1.17e+04±2.5 | 2.92e+03±6.8e+02 | 0 | 1 | 1 | 0.806±0.34 | 0 | 0 | 0 |
| structured_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 0.5 | 0.38 | 0.7846 | 0 | 1 | 1 | 46.8±7.3 | 2 | 1.18e+04±8.3 | 1.25e+03±24 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| structured_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 0.646±0.25 | 0.38 | 0.7846 | 0 | 0.667±0.58 | 1 | 556±4.2e+02 | 2 | 1.17e+04±9.5 | 5.38e+03±2.1e+03 | 0 | 1 | 1 | 1 | 0.861±0.24 | 1.67±0.58 | 0 |
| structured_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 0.354±0.25 | 0.45 | 0.9615 | 0 | 0.667±0.58 | 1 | 64.1±22 | 2 | 1.17e+04±9 | 2.8e+03±3.5e+02 | 0 | 1 | 1 | 1 | 0.806±0.34 | 2±1 | 0 |
| structured_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | - | - | - | 0 | 0 | 1 | 44.9±3.8 | 2 | 1.17e+04±6.7 | 2.6e+03±5.1e+02 | 0 | 1 | 1 | 0.861±0.24 | 0 | 0 | 0 |

## sp1_users

| spec | model | n | preservation_row_ratio_mean | preservation_null_inflation_mean | preservation_distinct_ratio_mean | preservation_empty_tables | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | manifest_present | manifest_table_coverage | manifest_column_coverage | row_count_match | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | -0.00595±0.01 | 0.96±0.014 | 0 | 1 | 1 | 84.9±0.96 | 4 | 5.54e+03±38 | 1.39e+03±31 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| critique_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 151±26 | 2 | 2.9e+03±27 | 4.69e+03±8.2e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| critique_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 152±27 | 4 | 6.09e+03±33 | 4.76e+03±8.6e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| critique_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0 | 0.9524 | 0 | 0.333±0.58 | 1 | 178±1.3e+02 | 4±1.7 | 6.15e+03±2.8e+03 | 9.38e+03±8.4e+03 | 0 | 1 | 1 | 1 | 1 | 0.333±0.58 | 0.333±0.58 | 0 |
| ensemble_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 52.4±2.5 | 4 | 5.11e+03±15 | 1.71e+03±31 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| ensemble_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0 | 0.9524 | 0 | 0.667±0.58 | 1 | 238±10 | 4 | 5.58e+03±79 | 7.2e+03±2e+03 | 0 | 1 | 1 | 1 | 1 | 0.667±0.58 | 0.667±0.58 | 0 |
| ensemble_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 84.2±13 | 4 | 5.7e+03±17 | 4.44e+03±2.2e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| ensemble_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0 | 0.917±0.051 | 0 | 0.667±0.58 | 1 | 65.3±4.9 | 4 | 5.76e+03±33 | 4.31e+03±4.2e+02 | 0 | 1 | 1 | 1 | 1 | 0.667±0.58 | 0.667±0.58 | 0 |
| freeform_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | - | - | - | 0 | 0 | 0.667±0.58 | 553±36 | 38±1.4 | 6.69e+04±3.6e+03 | 2.35e+03±4.2e+02 | 0 | 0 | 0 | 0 | - | 0 | 0 | 0 |
| freeform_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | - | - | 0 | 1 | 1 | 264±69 | 3.67±0.58 | 9.19e+03±2.4e+03 | 1.77e+03±3.7e+02 | 0 | 0 | 0 | 0 | 1 | 1 | 1 | 0 |
| freeform_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | - | - | 0 | 1 | 1 | 137±29 | 5.33±0.58 | 1.38e+04±1.6e+03 | 1.51e+03±1.3e+02 | 0 | 0 | 0 | 0 | 1 | 1 | 1 | 0 |
| freeform_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | - | - | - | 0 | 0 | 0.333±0.58 | 883±3.6e+02 | 69 | 2.21e+05 | 1.488e+04 | 0 | 0 | 0 | 0 | - | 0 | 0 | 0 |
| mock_control | - | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 15.5±0.15 | - | - | - | - | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| plan_planner_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 55.6±1.8 | 2 | 2.57e+03±41 | 968±46 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| plan_planner_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 842±5.7e+02 | 2 | 2.99e+03±96 | 3.68e+03±1.1e+03 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| plan_planner_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 60±4.7 | 2 | 2.81e+03±51 | 1.78e+03±2.2e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| plan_planner_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | 0 | 0.929±0.034 | 0 | 0.667±0.58 | 1 | 67.2±16 | 2 | 2.81e+03±34 | 2.58e+03±1.1e+03 | 0 | 1 | 1 | 1 | 1 | 0.667±0.58 | 0.667±0.58 | 0 |
| structured_sc_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 40.2±0.22 | 1 | 1.21e+03±2.3 | 543±6.1 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| structured_sc_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 298±2.8e+02 | 1 | 1.37e+03±3.6 | 3.26e+03±3.2e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| structured_sc_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 67.6±18 | 1 | 1.38e+03±0.58 | 1.16e+03±1e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| structured_sc_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | -0.0268±0.038 | 1.01±0.079 | 0 | 0.667±0.58 | 1 | 46.7±12 | 1 | 1.38e+03±3.5 | 1.08e+03±49 | 0 | 1 | 1 | 0.667±0.58 | 1 | 0.667±0.58 | 0.667±0.58 | 0 |
| structured_writer_model_devstral123 | academic/devstral-2-123b-instruct-2512 | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 51.2±13 | 1 | 1.21e+03±5.6 | 552±5.2 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| structured_writer_model_qwen36 | ise-ollama/qwen3.6:35b-a3b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 98.2±39 | 1 | 1.37e+03±3.6 | 1.97e+03±9.3e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| structured_writer_model_qwen397 | academic/qwen3.5-397b-a17b | 3 | 1 | 0 | 0.9524 | 0 | 1 | 1 | 44.7±17 | 1 | 1.38e+03±1.5 | 1.1e+03±1.5e+02 | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 0 |
| structured_writer_model_qwen9b | ise-openai-nvidia/qwen35-9b | 3 | 1 | -0.0119±0.021 | 0.983±0.027 | 0 | 1 | 1 | 550±12 | 1 | 1.38e+03±3.8 | 1.26e+03±1.2e+02 | 0 | 1 | 1 | 0.714±0.49 | 1 | 1 | 1 | 0 |

# Strategy × model — target_columns_covered (mean)

## sf_pipedrive_snapshot

| strategy | academic/devstral-2-123b-instruct-2512 | academic/qwen3.5-397b-a17b | ise-ollama/qwen3.6:35b-a3b | ise-openai-nvidia/qwen35-9b |
|---|---|---|---|---|
| ensemble_vote | 0.667 | 1 | 0.667 | - |
| plan_then_execute | 1 | 1 | 0.194 | 0.139 |
| structured_per_table | 1 | 0.903 | 0.694 | 0 |
| write_then_critique | 1 | 1 | 1 | 0 |
| write_tools_freeform | 1 | 1 | 0.861 | 0.667 |

## sp1_users

| strategy | - | academic/devstral-2-123b-instruct-2512 | academic/qwen3.5-397b-a17b | ise-ollama/qwen3.6:35b-a3b | ise-openai-nvidia/qwen35-9b |
|---|---|---|---|---|---|
| ensemble_vote | - | 1 | 1 | 0.667 | 0.667 |
| mock | 1 | - | - | - | - |
| plan_then_execute | - | 1 | 1 | 1 | 0.667 |
| structured_per_table | - | 1 | 1 | 1 | 0.833 |
| write_then_critique | - | 1 | 1 | 1 | 0.333 |
| write_tools_freeform | - | 0 | 1 | 1 | 0 |
