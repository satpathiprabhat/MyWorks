# app/src/env_loader.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Iterable, List, Optional, Callable, Any
from dotenv import load_dotenv

# Prevent accidental double loads
_LOADED = False

def _app_root() -> Path:
    # .../profile_connector/app/src/env_loader.py -> .../profile_connector/app
    return Path(__file__).resolve().parents[1]

def _project_root() -> Path:
    # assume project root is parent of app/
    return _app_root().parent

def _candidates(env: Optional[str]) -> List[Path]:
    """
    Build an ordered list of .env files to try. First existing takes effect,
    unless override=True (then later files can override earlier values).
    """
    app = _app_root()
    proj = _project_root()
    cfg = app / "config"

    # allow explicit override through ENV_FILE
    explicit = os.getenv("ENV_FILE")
    candidates: List[Path] = []
    if explicit:
        candidates.append(Path(explicit))

    # common names
    base = [
        Path.cwd() / ".env",              # when running from anywhere
        proj / ".env",                    # project root .env
        cfg / "dbconfig.env",             # your current file
        app / ".env",                     # optional: app-local
        proj / ".env.local",              # machine-local overrides (not in VCS)
    ]

    # profile-specific (dev/test/prod etc.)
    prof = []
    if env:
        # support multiple naming styles
        prof = [
            proj / f".env.{env}",
            proj / f".env.{env}.local",
            cfg / f"dbconfig.{env}.env",
        ]

    return [p for p in [*base, *prof] if p is not None]

def load_environment(
    env: Optional[str] = None,
    *,
    override: bool = False,
    required: Optional[Iterable[str]] = None,
    reload: bool = False,
    quiet: bool = False,
) -> List[Path]:
    """
    Load environment variables using a robust fallback chain.

    Parameters
    ----------
    env : str | None
        Optional profile (e.g., "dev", "test", "prod"). Tries .env.<env> etc.
    override : bool
        If True, later files override earlier values. If False, first hit wins.
    required : Iterable[str] | None
        Keys that must be present after loading; raises KeyError if missing.
    reload : bool
        Force reload even if already loaded once.
    quiet : bool
        Suppress informative prints.

    Returns
    -------
    List[Path]
        The list of files that actually existed (and were attempted).
    """
    global _LOADED
    if _LOADED and not reload:
        return []

    paths = _candidates(env)
    existing = [p for p in paths if p.exists()]

    # If override=False, we want "first existing file wins".
    # python-dotenv doesn't give us that directly, so we load in reverse
    # (so the earliest file loads last) unless override=True.
    to_load = existing if override else list(reversed(existing))

    loaded_any = False
    for p in to_load:
        ok = load_dotenv(p, override=override)
        if ok and not quiet:
            print(f"[env] loaded: {p}")
        loaded_any = loaded_any or ok

    _LOADED = True

    if not loaded_any and not quiet:
        print("[env] WARNING: no .env files found. Using process env only.")

    if required:
        missing = [k for k in required if os.getenv(k) in (None, "")]
        if missing:
            raise KeyError(f"Missing required env vars: {', '.join(missing)}")

    return existing

def get_env(
    key: str,
    default: Any = None,
    cast: Optional[Callable[[str], Any]] = None,
) -> Any:
    """
    Get an environment variable with optional casting.

    Examples:
        port = get_env("PORT", 5432, int)
        debug = get_env("DEBUG", False, lambda v: v.lower() in {"1","true","yes"})
    """
    val = os.getenv(key)
    if val is None:
        return default
    if cast:
        try:
            return cast(val)
        except Exception:
            raise ValueError(f"Failed to cast env '{key}' with value '{val}'")
    return val
