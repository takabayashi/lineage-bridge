# Copyright 2026 Daniel Takabayashi
# Licensed under the Apache License, Version 2.0
"""Sidebar — credential modals (Schema Registry, Flink, per-cluster).

Phase A redesign: credentials no longer live in nested sidebar expanders.
Each environment / cluster gets a "Manage credentials" button in the scope
section that opens a focused `st.dialog`. The session-state shape is
preserved verbatim so `_resolve_extraction_context` (in actions.py) keeps
working.

Status pill priority:

    explicit (green)  per-env / per-cluster keys set in this session
                      (either typed in the dialog or restored from cache)
    global  (blue)    no per-env/per-cluster keys, but a global key is
                      available in .env — extraction will work via fallback
    missing (grey)    nothing — extraction will fail unless the user
                      provides a key
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from lineage_bridge.config.cache import load_cache, update_cache


def _live_disk_creds() -> tuple[dict, dict, dict]:
    """Return ``(sr, flink, cluster)`` cred dicts read straight off disk.

    The session-state mirrors (`_cached_*_creds`) are seeded once in
    `state.load_cached_selections` and don't refresh — so if the user
    runs `make demo-up` while the app is open, the new keys never
    reach the dialog. Re-reading from disk on every seed call keeps
    the Manage Credentials dialog in sync with whatever's actually
    persisted, at the cost of one cheap JSON+Fernet decrypt per render.
    """
    disk = load_cache()
    return (
        disk.get("sr_credentials") or {},
        disk.get("flink_credentials") or {},
        disk.get("cluster_credentials") or {},
    )


def _seed_env_state(eid: str) -> None:
    """Pre-fill SR/Flink session keys for *eid* from the encrypted cache.

    Reads from BOTH the session-state mirror AND the live on-disk cache —
    whichever has values wins. Skips keys that already have a non-empty
    value in session state so user typing isn't clobbered.
    """
    disk_sr, disk_flink, _ = _live_disk_creds()
    cached_sr = {
        **disk_sr.get(eid, {}),
        **(st.session_state.get("_cached_sr_creds", {}).get(eid, {}) or {}),
    }
    cached_flink = {
        **disk_flink.get(eid, {}),
        **(st.session_state.get("_cached_flink_creds", {}).get(eid, {}) or {}),
    }
    for key, val in (
        (f"sr_endpoint_{eid}", cached_sr.get("endpoint")),
        (f"sr_key_{eid}", cached_sr.get("api_key")),
        (f"sr_secret_{eid}", cached_sr.get("api_secret")),
        (f"flink_key_{eid}", cached_flink.get("api_key")),
        (f"flink_secret_{eid}", cached_flink.get("api_secret")),
    ):
        if val and not st.session_state.get(key):
            st.session_state[key] = val


def _seed_audit_log_state() -> None:
    """Pre-fill the watcher's audit-log inputs from the encrypted cache.

    Mirror of `_seed_env_state` for the single audit-log bundle: read the
    on-disk cache on every render so freshly-saved values are picked up
    without restarting the app, and skip keys the user has already typed
    into so we don't clobber in-flight edits.
    """
    bundle = load_cache().get("audit_log_credentials") or {}
    for key, val in (
        ("watcher_audit_bootstrap", bundle.get("bootstrap_servers")),
        ("watcher_audit_key", bundle.get("api_key")),
        ("watcher_audit_secret", bundle.get("api_secret")),
    ):
        if val and not st.session_state.get(key):
            st.session_state[key] = val


def _seed_cluster_state(cid: str) -> None:
    """Pre-fill cluster session keys for *cid* from the encrypted cache."""
    _, _, disk_cluster = _live_disk_creds()
    cached = {
        **disk_cluster.get(cid, {}),
        **(st.session_state.get("_cached_cluster_creds", {}).get(cid, {}) or {}),
    }
    for key, val in (
        (f"cluster_key_{cid}", cached.get("api_key")),
        (f"cluster_secret_{cid}", cached.get("api_secret")),
    ):
        if val and not st.session_state.get(key):
            st.session_state[key] = val


def env_creds_status(env_id: str, has_flink: bool, settings: Any | None = None) -> tuple[str, str]:
    """Return (status, label) for an environment's credentials.

    status: ``"explicit"`` | ``"global"`` | ``"missing"``
    label: human-readable summary that names which sources are in play.
    """
    sr_endpoint = (st.session_state.get(f"sr_endpoint_{env_id}", "") or "").strip()
    sr_key = st.session_state.get(f"sr_key_{env_id}", "") or ""
    sr_secret = st.session_state.get(f"sr_secret_{env_id}", "") or ""
    flink_key = st.session_state.get(f"flink_key_{env_id}", "") or ""
    flink_secret = st.session_state.get(f"flink_secret_{env_id}", "") or ""

    sr_explicit = bool(sr_endpoint and sr_key and sr_secret)
    flink_explicit = bool(flink_key and flink_secret)

    sr_global = bool(
        settings
        and getattr(settings, "schema_registry_endpoint", None)
        and getattr(settings, "schema_registry_api_key", None)
        and getattr(settings, "schema_registry_api_secret", None)
    )
    flink_global = bool(
        settings
        and getattr(settings, "flink_api_key", None)
        and getattr(settings, "flink_api_secret", None)
    )

    parts: list[str] = []
    if sr_explicit:
        parts.append("SR")
    elif sr_global:
        parts.append("SR (global)")
    if has_flink:
        if flink_explicit:
            parts.append("Flink")
        elif flink_global:
            parts.append("Flink (global)")

    if not parts:
        return "missing", "No SR / Flink keys configured"

    has_explicit = sr_explicit or (has_flink and flink_explicit)
    if has_explicit:
        return "explicit", " + ".join(parts)
    return "global", " + ".join(parts)


def audit_log_creds_status(settings: Any | None = None) -> tuple[str, str]:
    """Return (status, label) for the watcher's audit-log credentials.

    Same three-tone scheme as env / cluster: explicit (typed in dialog or
    restored from cache) > global (.env fallback) > missing.
    """
    bs = (st.session_state.get("watcher_audit_bootstrap", "") or "").strip()
    ak = st.session_state.get("watcher_audit_key", "") or ""
    asec = st.session_state.get("watcher_audit_secret", "") or ""

    if bs and ak and asec:
        return "explicit", "Audit log key cached"

    global_set = bool(
        settings
        and getattr(settings, "audit_log_bootstrap_servers", None)
        and getattr(settings, "audit_log_api_key", None)
        and getattr(settings, "audit_log_api_secret", None)
    )
    if global_set:
        return "global", "Using audit log keys from `.env`"

    return "missing", "No audit log keys configured"


def cluster_creds_status(cluster_id: str, settings: Any | None = None) -> tuple[str, str]:
    """Return (status, label) for a cluster's credentials."""
    key = st.session_state.get(f"cluster_key_{cluster_id}", "") or ""
    secret = st.session_state.get(f"cluster_secret_{cluster_id}", "") or ""

    if key and secret:
        return "explicit", "Cluster key set"

    cluster_creds_map = getattr(settings, "cluster_credentials", {}) or {}
    if cluster_id in cluster_creds_map:
        return "explicit", "Cluster key from .env"

    cloud_key = getattr(settings, "confluent_cloud_api_key", None)
    if cloud_key:
        return "global", "Using Confluent Cloud key"

    return "missing", "No key configured"


# ── Dialog status helpers ──────────────────────────────────────────────


def _render_env_dialog_status(env_id: str, has_flink: bool, settings: Any | None) -> None:
    """Banner inside the env dialog: which keys are about to be used.

    Distinguishes between:
    - **explicit** override already typed for this env  (green info)
    - **global** .env fallback will be used             (blue info)
    - **missing**: nothing configured                   (orange warning)

    Per-section so the SR and Flink stories don't collide.
    """
    sr_explicit = bool(
        st.session_state.get(f"sr_key_{env_id}") and st.session_state.get(f"sr_secret_{env_id}")
    )
    sr_global = bool(
        settings
        and getattr(settings, "schema_registry_endpoint", None)
        and getattr(settings, "schema_registry_api_key", None)
        and getattr(settings, "schema_registry_api_secret", None)
    )

    flink_explicit = bool(
        st.session_state.get(f"flink_key_{env_id}")
        and st.session_state.get(f"flink_secret_{env_id}")
    )
    flink_global = bool(
        settings
        and getattr(settings, "flink_api_key", None)
        and getattr(settings, "flink_api_secret", None)
    )

    parts: list[str] = []
    if sr_explicit:
        parts.append("✓ Schema Registry: per-env override set")
    elif sr_global:
        parts.append("→ Schema Registry: using global `.env` keys at extraction")
    else:
        parts.append("⚠ Schema Registry: no keys configured")

    if has_flink:
        if flink_explicit:
            parts.append("✓ Flink: per-env override set")
        elif flink_global:
            parts.append("→ Flink: using global `.env` keys at extraction")
        else:
            parts.append("⚠ Flink: no keys configured")

    has_warning = any(p.startswith("⚠") for p in parts)
    body = "  \n".join(parts)
    if has_warning:
        st.warning(body)
    else:
        st.info(body)


def _render_cluster_dialog_status(cluster_id: str, settings: Any | None) -> None:
    """Banner inside the cluster dialog: which key will be used."""
    explicit = bool(
        st.session_state.get(f"cluster_key_{cluster_id}")
        and st.session_state.get(f"cluster_secret_{cluster_id}")
    )
    cloud_global = bool(settings and getattr(settings, "confluent_cloud_api_key", None))
    cluster_creds_map = getattr(settings, "cluster_credentials", {}) if settings else {}
    in_settings_map = cluster_id in (cluster_creds_map or {})

    if explicit:
        st.info("✓ Per-cluster override set")
    elif in_settings_map:
        st.info("→ Using cluster key from `.env` (`LINEAGE_BRIDGE_CLUSTER_CREDENTIALS`)")
    elif cloud_global:
        st.info("→ Using global Confluent Cloud key at extraction")
    else:
        st.warning("⚠ No keys configured")


# ── Dialogs ─────────────────────────────────────────────────────────────


@st.dialog("Environment credentials")
def env_credentials_dialog(env_id: str, env_name: str, has_flink: bool) -> None:
    """Modal for one environment's SR + Flink credentials.

    Inputs are bound to the same session-state keys as the legacy expander
    (`sr_endpoint_{eid}`, `sr_key_{eid}`, etc.) so the rest of the
    extraction pipeline reads them unchanged.
    """
    from lineage_bridge.ui.discovery import _try_load_settings

    _seed_env_state(env_id)
    settings = _try_load_settings()

    st.caption(
        f"Credentials for **{env_name}** (`{env_id}`). "
        "Leave any field blank to fall back to global keys from `.env`."
    )

    # Status banner: explain what will happen when fields are blank, so an
    # empty dialog isn't read as "nothing is configured" when in fact the
    # global .env keys would be used at extraction time.
    _render_env_dialog_status(env_id, has_flink, settings)

    st.markdown("**Schema Registry**")
    st.text_input(
        "Endpoint",
        key=f"sr_endpoint_{env_id}",
        placeholder="https://psrc-xxxxx.region.cloud.confluent.cloud",
    )
    c1, c2 = st.columns(2)
    with c1:
        st.text_input("Key", key=f"sr_key_{env_id}", type="password")
    with c2:
        st.text_input("Secret", key=f"sr_secret_{env_id}", type="password")

    st.markdown("**Flink**")
    if not has_flink:
        st.caption("No Flink compute pool discovered for this environment.")
    c3, c4 = st.columns(2)
    with c3:
        st.text_input(
            "Flink Key",
            key=f"flink_key_{env_id}",
            type="password",
            disabled=not has_flink,
        )
    with c4:
        st.text_input(
            "Flink Secret",
            key=f"flink_secret_{env_id}",
            type="password",
            disabled=not has_flink,
        )

    st.divider()
    bcol1, bcol2 = st.columns([1, 1])
    with bcol1:
        if st.button(
            "Clear all",
            key=f"clear_env_creds_{env_id}",
            type="secondary",
            width="stretch",
        ):
            for suffix in ("sr_endpoint", "sr_key", "sr_secret", "flink_key", "flink_secret"):
                st.session_state[f"{suffix}_{env_id}"] = ""
            st.rerun()
    with bcol2:
        if st.button(
            "Done",
            key=f"close_env_creds_{env_id}",
            type="primary",
            width="stretch",
        ):
            st.rerun()


@st.dialog("Audit log credentials")
def audit_log_credentials_dialog() -> None:
    """Modal for the watcher's audit-log Kafka cluster credentials.

    Saves to the encrypted cache on Done so the bundle survives session
    restarts — this is why the audit-log dialog persists eagerly while the
    env / cluster dialogs lazy-save through extraction. Without persistence,
    every restart would force the user to re-enter audit-log creds even
    though they're stable per-org.
    """
    from lineage_bridge.ui.discovery import _try_load_settings

    _seed_audit_log_state()
    settings = _try_load_settings()

    st.caption(
        "Credentials for the org-wide audit-log Kafka cluster (see ADR-014). "
        "Leave blank to fall back to `LINEAGE_BRIDGE_AUDIT_LOG_*` from `.env`."
    )

    status, label = audit_log_creds_status(settings)
    if status == "explicit":
        st.info(f"✓ {label}")
    elif status == "global":
        st.info(f"→ {label}")
    else:
        st.warning(f"⚠ {label}")

    st.text_input(
        "Bootstrap servers",
        key="watcher_audit_bootstrap",
        placeholder="pkc-xxxxx.region.cloud.confluent.cloud:9092",
    )
    c1, c2 = st.columns(2)
    with c1:
        st.text_input("API Key", key="watcher_audit_key", type="password")
    with c2:
        st.text_input("API Secret", key="watcher_audit_secret", type="password")

    st.divider()
    bcol1, bcol2 = st.columns([1, 1])
    with bcol1:
        if st.button(
            "Clear",
            key="clear_audit_log_creds",
            type="secondary",
            width="stretch",
        ):
            for key in ("watcher_audit_bootstrap", "watcher_audit_key", "watcher_audit_secret"):
                st.session_state[key] = ""
            # Wipe the persisted bundle too — Clear means clear, not "blank
            # the form but keep the cached values that re-seed it on render".
            update_cache(audit_log_credentials={})
            st.rerun()
    with bcol2:
        if st.button(
            "Save",
            key="save_audit_log_creds",
            type="primary",
            width="stretch",
        ):
            bs = (st.session_state.get("watcher_audit_bootstrap", "") or "").strip()
            ak = st.session_state.get("watcher_audit_key", "") or ""
            asec = st.session_state.get("watcher_audit_secret", "") or ""
            if bs and ak and asec:
                update_cache(
                    audit_log_credentials={
                        "bootstrap_servers": bs,
                        "api_key": ak,
                        "api_secret": asec,
                    }
                )
            st.rerun()


@st.dialog("Cluster credentials")
def cluster_credentials_dialog(cluster_id: str, cluster_name: str) -> None:
    """Modal for one cluster's API key + secret."""
    from lineage_bridge.ui.discovery import _try_load_settings

    _seed_cluster_state(cluster_id)
    settings = _try_load_settings()

    st.caption(
        f"Optional cluster-scoped key for **{cluster_name}** (`{cluster_id}`). "
        "Leave blank to use the global key."
    )

    _render_cluster_dialog_status(cluster_id, settings)

    c1, c2 = st.columns(2)
    with c1:
        st.text_input("API Key", key=f"cluster_key_{cluster_id}", type="password")
    with c2:
        st.text_input("API Secret", key=f"cluster_secret_{cluster_id}", type="password")

    st.divider()
    bcol1, bcol2 = st.columns([1, 1])
    with bcol1:
        if st.button(
            "Clear",
            key=f"clear_cluster_creds_{cluster_id}",
            type="secondary",
            width="stretch",
        ):
            st.session_state[f"cluster_key_{cluster_id}"] = ""
            st.session_state[f"cluster_secret_{cluster_id}"] = ""
            st.rerun()
    with bcol2:
        if st.button(
            "Done",
            key=f"close_cluster_creds_{cluster_id}",
            type="primary",
            width="stretch",
        ):
            st.rerun()


# ── Inline rows (called from scope.py) ──────────────────────────────────


def render_env_credentials_row(env: Any, has_flink: bool, settings: Any | None = None) -> None:
    """Inline row for one env: pill + 'Manage' button. Opens dialog on click.

    Eagerly seeds cached creds into session state before computing status, so
    the pill reflects "configured from cache" on first render rather than
    waiting until the user opens the dialog.
    """
    _seed_env_state(env.id)
    status, label = env_creds_status(env.id, has_flink, settings)
    pill = _status_pill_html(status, label)

    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown(
            f"<div class='creds-row-name'>{env.display_name}</div>{pill}",
            unsafe_allow_html=True,
        )
    with c2:
        if st.button(
            "Manage",
            key=f"manage_env_creds_btn_{env.id}",
            width="stretch",
        ):
            env_credentials_dialog(env.id, env.display_name, has_flink)


def render_cluster_credentials_row(cluster: Any, settings: Any | None = None) -> None:
    """Inline row for one cluster: pill + 'Manage' button."""
    _seed_cluster_state(cluster.id)
    status, label = cluster_creds_status(cluster.id, settings)
    pill = _status_pill_html(status, label)

    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown(
            f"<div class='creds-row-name'>{cluster.display_name}</div>{pill}",
            unsafe_allow_html=True,
        )
    with c2:
        if st.button(
            "Manage",
            key=f"manage_cluster_creds_btn_{cluster.id}",
            width="stretch",
        ):
            cluster_credentials_dialog(cluster.id, cluster.display_name)


def _status_pill_html(status: str, label: str) -> str:
    """Three-tone pill: green = explicit, blue = global fallback, grey = missing."""
    if status == "explicit":
        css_class = "creds-pill-explicit"
        dot = "#4CAF50"
    elif status == "global":
        css_class = "creds-pill-global"
        dot = "#1976D2"
    else:
        css_class = "creds-pill-missing"
        dot = "#9E9E9E"
    return (
        f"<div class='creds-pill {css_class}'>"
        f"<span class='status-dot' style='background:{dot}'></span>"
        f"<span>{label}</span>"
        f"</div>"
    )
