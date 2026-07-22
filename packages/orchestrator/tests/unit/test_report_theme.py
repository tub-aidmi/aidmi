def test_theme_and_lookups():
    import matplotlib

    matplotlib.use("Agg")
    from aidmi_orchestrator.report import theme

    theme.apply_theme()
    assert theme.color_for_cell("write_tools_freeform").startswith("#")
    assert theme.color_for_cell("unknown_cell").startswith("#")  # fallback
    assert theme.marker_for_model("gemini25flash")
