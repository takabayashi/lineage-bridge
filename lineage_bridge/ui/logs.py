# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Bottom-drawer log viewer for extraction and push activity.

Replaces the in-sidebar log expanders, which were too narrow for multi-line
entries and buried under several other controls. The drawer lives at the
bottom of the main area, defaults collapsed, and shows a one-line summary
strip when either log has content. Expanded view has filter chips, search,
severity counts, and a download button — full page width.

The `@st.fragment(run_every=N)` live-refresh experiment was removed: the
fragment ID becomes stale across the `st.rerun()` that follows every
extract/push action, producing a noisy "fragment does not exist anymore"
warning. The log now appears once after the action completes, which is the
normal Streamlit pattern.
"""

from __future__ import annotations

import streamlit as st

# Severity classes — kept narrow so filter chips stay scanable.
_SEVERITIES: tuple[str, ...] = ("phase", "discovery", "provision", "warning", "error", "skip")


def classify_log_entry(line: str) -> tuple[str, str, str, str, str]:
    """Parse one log line into (timestamp, severity, css_class, icon, html_text).

    Lines have the shape ``[HH:MM:SS] **Label** detail``. Both the timestamp
    and the bolded label are optional. Severity is derived from the label
    using substring matches.
    """
    text = line.strip()
    ts = ""
    if text.startswith("[") and "]" in text:
        end = text.index("]")
        ts_candidate = text[1:end]
        if len(ts_candidate) == 8 and ts_candidate[2] == ":" and ts_candidate[5] == ":":
            ts = ts_candidate
            text = text[end + 1 :].lstrip()

    label = ""
    if text.startswith("**"):
        end_idx = text.find("**", 2)
        if end_idx > 2:
            label = text[2:end_idx]
            text = text[end_idx + 2 :].strip()

    ll = label.lower()
    text_lower = text.lower()

    if "error" in ll or "failed" in ll or "exception" in ll:
        return ts, "error", "log-error", "✕", _wrap_label(label, text)
    if "warning" in ll or "warn" in text_lower[:32]:
        return ts, "warning", "log-warning", "⚠", _wrap_label(label, text)
    if "skip" in ll:
        return ts, "skip", "log-skip", "⏭", _wrap_label(label, text)
    if "discover" in ll:
        return ts, "discovery", "log-discovery", "\U0001f50d", _wrap_label(label, text)
    if "provision" in ll:
        return ts, "provision", "log-provision", "\U0001f511", _wrap_label(label, text)
    if "phase" in ll:
        return ts, "phase", "log-phase", "▶", _wrap_label(label, text)
    return ts, "phase", "log-phase", "•", _wrap_label(label, text)


def _wrap_label(label: str, text: str) -> str:
    """Re-emit the label as a styled span before the prose."""
    if not label:
        return text
    return f"<span class='log-label'>{label}</span>{text}"


def severity_counts(lines: list[str]) -> dict[str, int]:
    counts = dict.fromkeys(_SEVERITIES, 0)
    for line in lines:
        _, sev, *_ = classify_log_entry(line)
        counts[sev] = counts.get(sev, 0) + 1
    return counts


def render_logs_drawer() -> None:
    """One-stop bottom drawer for extraction + push logs.

    Compact when collapsed (single-line summary); expanded shows the full
    filtered log viewer. Hidden entirely when both logs are empty.
    """
    ext = st.session_state.get("extraction_log") or []
    push = st.session_state.get("push_log") or []
    if not (ext or push):
        return

    sources: list[tuple[str, str, list[str]]] = []
    if ext:
        sources.append(("extraction", "Extraction", ext))
    if push:
        sources.append(("push", "Push", push))

    summary_html = _build_summary_html(sources)

    st.markdown("<div class='logs-anchor'></div>", unsafe_allow_html=True)
    with st.expander(
        label=f"\U0001f4cb Activity log — {summary_html.text_summary}", expanded=False
    ):
        # Source switcher — only when both logs have content
        if len(sources) > 1:
            choice = st.segmented_control(
                "Source",
                [s[1] for s in sources],
                default=sources[0][1],
                key="logs_drawer_source",
                label_visibility="collapsed",
            )
            chosen = next(s for s in sources if s[1] == choice)
        else:
            chosen = sources[0]

        state_key, _label, lines = chosen

        # Header summary inside the drawer (counts + clear button)
        h_left, h_right = st.columns([5, 1])
        with h_left:
            st.markdown(summary_html.html, unsafe_allow_html=True)
        with h_right:
            if st.button("Clear", key=f"clear_{state_key}_btn", width="stretch"):
                st.session_state[state_key] = []
                st.rerun()

        _render_log_body(state_key, lines)


class _Summary:
    """Container for the two summary representations the drawer label needs."""

    def __init__(self, text_summary: str, html: str) -> None:
        self.text_summary = text_summary
        self.html = html


def _build_summary_html(sources: list[tuple[str, str, list[str]]]) -> _Summary:
    """Build collapsed-label summary string + expanded-header HTML."""
    text_parts: list[str] = []
    html_parts: list[str] = []
    has_alert = False
    for _key, label, lines in sources:
        counts = severity_counts(lines)
        n = len(lines)
        warn = counts.get("warning", 0)
        err = counts.get("error", 0)
        if warn or err:
            has_alert = True

        bits = [f"{n} entry" if n == 1 else f"{n} entries"]
        if err:
            bits.append(f"{err} error{'' if err == 1 else 's'}")
        if warn:
            bits.append(f"{warn} warning{'' if warn == 1 else 's'}")
        if counts.get("skip"):
            bits.append(f"{counts['skip']} skipped")

        text_parts.append(f"{label} · {' · '.join(bits)}")

        warn_html = f" &middot; <span class='logs-alert'>⚠ {warn}</span>" if warn else ""
        err_html = f" &middot; <span class='logs-alert'>✕ {err}</span>" if err else ""
        html_parts.append(
            f"<span class='logs-summary-pill'>"
            f"<strong>{label}</strong> {n}"
            f"{warn_html}"
            f"{err_html}"
            f"</span>"
        )

    label_class = "logs-summary logs-summary-alert" if has_alert else "logs-summary"
    return _Summary(
        text_summary=" · ".join(text_parts),
        html=f"<div class='{label_class}'>" + "".join(html_parts) + "</div>",
    )


def _render_log_body(state_key: str, lines: list[str]) -> None:
    """Render the filtered log list with chips + search + download."""
    if not lines:
        st.caption("Empty.")
        return

    # ── Toolbar: filter chips + search ────────────────────────────────
    c1, c2 = st.columns([3, 2])
    with c1:
        chip_options = ["All", "Phases", "Warnings", "Errors", "Skipped"]
        chip = st.segmented_control(
            "Filter",
            chip_options,
            default="All",
            key=f"{state_key}_chip",
            label_visibility="collapsed",
        )
    with c2:
        query = st.text_input(
            "Search log",
            placeholder="Search...",
            key=f"{state_key}_search",
            label_visibility="collapsed",
        )

    chip_filter = {
        "Phases": {"phase", "discovery", "provision"},
        "Warnings": {"warning"},
        "Errors": {"error"},
        "Skipped": {"skip"},
    }.get(chip)

    query_lower = query.strip().lower() if query else ""

    # ── Render filtered entries ───────────────────────────────────────
    html_parts: list[str] = []
    rendered = 0
    for line in lines:
        ts, sev, css_class, icon, html_text = classify_log_entry(line)
        if chip_filter is not None and sev not in chip_filter:
            continue
        if query_lower and query_lower not in line.lower():
            continue
        ts_html = f"<span class='log-time'>{ts}</span>" if ts else ""
        html_parts.append(
            f"<div class='log-entry {css_class}'>"
            f"{ts_html}"
            f"<span class='log-icon'>{icon}</span>"
            f"<span class='log-text'>{html_text}</span>"
            f"</div>"
        )
        rendered += 1

    if rendered == 0:
        st.caption("No entries match the current filter.")
    else:
        st.markdown(
            f"<div class='log-list'>{''.join(html_parts)}</div>",
            unsafe_allow_html=True,
        )

    # Download
    st.download_button(
        "Download full log",
        data="\n".join(lines),
        file_name=f"{state_key}.txt",
        mime="text/plain",
        key=f"download_{state_key}",
        width="stretch",
    )
