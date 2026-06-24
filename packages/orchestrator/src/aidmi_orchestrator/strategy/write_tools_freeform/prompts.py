from aidmi_orchestrator.strategy.guidelines.compose import freeform_system_prompt

SYSTEM_PROMPT = freeform_system_prompt(enable_self_correction=False)


def build_system_prompt(*, enable_self_correction: bool = False) -> str:
    return freeform_system_prompt(enable_self_correction=enable_self_correction)


def build_initial_user_prompt(context: str, *, enable_self_correction: bool = False) -> str:
    prompt = (
        f"{context}\n\n"
        f"Produce the dbt project that transforms the source into the target."
    )
    if enable_self_correction:
        prompt += (
            "\n\nSelf-correction is ON: after writing models, call run_dbt(), "
            "fix any errors, and read back every model file you edit before finishing."
        )
    return prompt


def initial_user_prompt(context: str) -> str:
    return build_initial_user_prompt(context)
