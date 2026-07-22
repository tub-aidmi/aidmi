"""Static, self-contained HTML gallery assembly.

`render_gallery` stitches pre-rendered SVG figures and pre-rendered HTML
tables (from `report/tables.py`) into a single navigable HTML document.
Pure stdlib string assembly -- no matplotlib, no pandas, no templating
engine, no external assets.

Table-placement design: a `Section` carries an optional `table_keys`
tuple. Each key is looked up in the `tables` dict passed to
`render_gallery` and the corresponding pre-rendered table HTML is
inserted verbatim, in the given order, after that section's figures.
The caller decides *which* table belongs in *which* section by setting
`table_keys` (e.g. `table_keys=("best_config",)` on the Headline
section); `render_gallery` has no built-in knowledge of specific table
names. This keeps the association explicit at the call site and keeps
this module ignorant of what the tables mean.

Nav/section ordering mirrors the order of the `sections` list as given
by the caller -- the caller is responsible for passing sections in the
canonical order (Headline, Metric choice, Levers, Strategy, Reliability,
Fixtures, [Cross-campaign], Appendix). The one exception `render_gallery`
enforces itself: any section with `id == "cross_campaign"` is dropped
entirely (from both nav and body) when `multi_model` is False, so the
"Cross-campaign" entry is guaranteed conditional even if a caller
forgets to omit it.
"""

from __future__ import annotations

import html as _html
from dataclasses import dataclass, field
from pathlib import Path

_CROSS_CAMPAIGN_ID = "cross_campaign"


@dataclass(frozen=True)
class Subsection:
    title: str
    figures: list[Path]


@dataclass(frozen=True)
class Section:
    id: str
    title: str
    figures: list[Path]
    caption: str
    table_keys: tuple[str, ...] = field(default_factory=tuple)
    # When True, figures stack one per row at full width instead of flowing
    # side-by-side in the capped-width grid. Use for wide, detail-dense figures
    # that need the whole column to be legible.
    stacked: bool = False
    # Optional h3-titled figure groups rendered after the flat figures; each
    # renders its own figures div. Honours the section's `stacked` layout.
    subsections: tuple[Subsection, ...] = field(default_factory=tuple)


def _esc(value: str) -> str:
    return _html.escape(str(value))


def _figure_label(path: Path) -> str:
    return path.stem.replace("_", " ").replace("-", " ").strip().capitalize()


def _render_figure(fig: Path, *, alt: str) -> str:
    src = f"figures/{Path(fig).name}"
    caption = _esc(_figure_label(fig))
    return (
        "<figure>"
        f'<img src="{_esc(src)}" alt="{alt}" loading="lazy">'
        f"<figcaption>{caption}</figcaption>"
        "</figure>"
    )


def _render_section(section: Section, tables: dict[str, str]) -> str:
    title = _esc(section.title)
    caption = _esc(section.caption)
    figures_html = "".join(_render_figure(f, alt=title) for f in section.figures)
    tables_html = "".join(tables[key] for key in section.table_keys if key in tables)
    figures_class = "figures figures--stacked" if section.stacked else "figures"
    caption_html = f'<p class="caption">{caption}</p>' if section.caption else ""
    flat_figures_html = (
        f'<div class="{figures_class}">{figures_html}</div>' if section.figures else ""
    )
    subsections_html = "".join(
        f"<h3>{_esc(sub.title)}</h3>"
        f'<div class="{figures_class}">'
        + "".join(_render_figure(f, alt=title) for f in sub.figures)
        + "</div>"
        for sub in section.subsections
    )
    return (
        "<section>"
        f'<h2 id="{_esc(section.id)}">{title}</h2>'
        f"{caption_html}"
        f"{flat_figures_html}"
        f"{subsections_html}"
        f"{tables_html}"
        "</section>"
    )


def _render_nav(sections: list[Section]) -> str:
    items = "".join(
        f'<li><a href="#{_esc(s.id)}">{_esc(s.title)}</a></li>' for s in sections
    )
    return f"<nav><ul>{items}</ul></nav>"


_STYLE = """
:root { color-scheme: light dark; }
body { font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
       margin: 0; line-height: 1.5; color: #1a1a1a; background: #fff; }
nav { position: sticky; top: 0; background: #f5f5f7; border-bottom: 1px solid #ddd;
      padding: 0.5rem 1rem; z-index: 10; }
nav ul { list-style: none; display: flex; flex-wrap: wrap; gap: 1rem; margin: 0; padding: 0; }
nav a { text-decoration: none; color: #1a5fb4; font-size: 0.9rem; }
nav a:hover { text-decoration: underline; }
main { max-width: 1100px; margin: 0 auto; padding: 1rem 1.5rem 3rem; }
h1 { margin: 1.5rem 0 0.25rem; }
h2 { margin-top: 2.5rem; padding-top: 0.5rem; border-top: 1px solid #ddd; }
h3 { margin: 1.5rem 0 0.5rem; font-size: 1.05rem; }
.caption { color: #444; font-style: italic; margin-top: 0; }
.figures { display: flex; flex-wrap: wrap; gap: 1rem; margin: 1rem 0; }
.figures--stacked { flex-direction: column; flex-wrap: nowrap; }
.figures--stacked figure { max-width: 100%; }
figure { margin: 0; max-width: 520px; }
figure img { max-width: 100%; height: auto; border: 1px solid #ddd; }
figcaption { font-size: 0.85rem; color: #555; margin-top: 0.25rem; }
table { border-collapse: collapse; margin: 1rem 0; width: 100%; }
caption { text-align: left; font-size: 0.85rem; color: #555; margin-bottom: 0.25rem; }
th, td { border: 1px solid #ddd; padding: 0.35rem 0.6rem; text-align: left; font-size: 0.9rem; }
th { background: #f5f5f7; }
@media (prefers-color-scheme: dark) {
  body { color: #e8e8e8; background: #1a1a1a; }
  nav { background: #242424; border-color: #3a3a3a; }
  nav a { color: #7ab0f0; }
  h2 { border-color: #3a3a3a; }
  .caption, figcaption, caption { color: #aaa; }
  figure img { border-color: #3a3a3a; }
  th, td { border-color: #3a3a3a; }
  th { background: #242424; }
}
"""


def render_gallery(
    *, title: str, sections: list[Section], tables: dict[str, str], multi_model: bool
) -> str:
    visible = [s for s in sections if s.id != _CROSS_CAMPAIGN_ID or multi_model]
    escaped_title = _esc(title)
    body_sections = "".join(_render_section(s, tables) for s in visible)
    return (
        "<!doctype html>"
        '<html lang="en"><head>'
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{escaped_title}</title>"
        f"<style>{_STYLE}</style>"
        "</head><body>"
        f"{_render_nav(visible)}"
        f"<main><h1>{escaped_title}</h1>{body_sections}</main>"
        "</body></html>"
    )
