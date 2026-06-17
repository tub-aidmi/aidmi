# Benchmark summary

## sf_pipedrive_snapshot

| spec | model | n | preservation_row_ratio_mean | preservation_empty_tables | dbt_success | ran_ok | wall_clock_seconds | llm_calls_total | tokens_input_total | tokens_output_total | dollar_cost_total | manifest_present | manifest_table_coverage | manifest_column_coverage | target_columns_covered | type_mismatches | extraneous_columns |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| critique_writer_model_gemma26 | gemma4:26b | 1 | 0.5 | 0 | 1 | 1 | 319.6 | 6 | 3.951e+04 | 1.298e+04 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| critique_writer_model_qwen36 | batiai/qwen3.6-35b:q4 | 1 | - | - | - | 0 | 247.2 | - | - | - | - | - | - | - | - | - | - |
| ensemble_writer_model_gemma26 | gemma4:26b | 1 | 0.5 | 0 | 1 | 1 | 648.2 | 14 | 9.188e+04 | 2.806e+04 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| ensemble_writer_model_qwen36 | batiai/qwen3.6-35b:q4 | 1 | - | - | - | 0 | 165.6 | - | - | - | - | - | - | - | - | - | - |
| freeform_writer_model_gemma26 | gemma4:26b | 1 | 0.5 | 0 | 1 | 1 | 77.75 | 5 | 3.588e+04 | 1806 | 0 | 0 | 0 | 0 | 1 | 2 | 0 |
| freeform_writer_model_qwen36 | batiai/qwen3.6-35b:q4 | 1 | 0.5 | 0 | 1 | 1 | 221.4 | 9 | 1.182e+05 | 8316 | 0 | 0 | 0 | 0 | 1 | 2 | 0 |
| plan_planner_model_gemma26 | gemma4:26b | 1 | 0.5 | 0 | 1 | 1 | 337.3 | 6 | 4.191e+04 | 1.261e+04 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| plan_planner_model_qwen36 | batiai/qwen3.6-35b:q4 | 1 | - | - | - | 0 | 126.7 | - | - | - | - | - | - | - | - | - | - |
| structured_writer_model_gemma26 | gemma4:26b | 1 | 0.5 | 0 | 1 | 1 | 223.3 | 4 | 2.627e+04 | 8302 | 0 | 1 | 1 | 1 | 1 | 2 | 0 |
| structured_writer_model_qwen36 | batiai/qwen3.6-35b:q4 | 1 | - | - | - | 0 | 109.7 | - | - | - | - | - | - | - | - | - | - |

# Strategy × model — target_columns_covered (mean)

## sf_pipedrive_snapshot

| strategy | batiai/qwen3.6-35b:q4 | gemma4:26b |
|---|---|---|
| ensemble_vote | - | 1 |
| plan_then_execute | - | 1 |
| structured_per_table | - | 1 |
| write_then_critique | - | 1 |
| write_tools_freeform | 1 | 1 |
