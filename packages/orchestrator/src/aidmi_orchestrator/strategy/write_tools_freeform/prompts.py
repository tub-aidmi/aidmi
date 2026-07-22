from aidmi_orchestrator.strategy.guidelines.compose import freeform_system_prompt

SYSTEM_PROMPT = freeform_system_prompt(enable_self_correction=False)


def build_system_prompt(
    *,
    enable_self_correction: bool = False,
    inline_run_dbt_tool: bool = False,
) -> str:
    return freeform_system_prompt(
        enable_self_correction=enable_self_correction,
        inline_run_dbt_tool=inline_run_dbt_tool,
    )


def build_initial_user_prompt(
    context: str,
    *,
    enable_self_correction: bool = False,
    inline_run_dbt_tool: bool = False,
) -> str:
    prompt = (
        f"{context}\n\n"
        f"Produce the dbt project that transforms the source into the target. "
        f"Write one model file per target table listed above — all tables are required."
    )
    if enable_self_correction and inline_run_dbt_tool:
        prompt += (
            "\n\nSelf-correction is ON: after writing models, call run_dbt(), "
            "fix any errors, and read back every model file you edit before finishing."
        )
    elif enable_self_correction:
        prompt += (
            "\n\nSelf-correction is ON: write complete models; the orchestrator "
            "will run dbt after you finish and prompt you to fix failures if needed."
        )
    return prompt
