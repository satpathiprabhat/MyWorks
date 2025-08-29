# app/logging_setup.py
from __future__ import annotations

import os
import logging
import logging.config
from pathlib import Path

try:
    import yaml  # pip install pyyaml
except Exception:
    yaml = None

def _project_root() -> Path:
    # adjust if your tree is different; this goes 1 level up from this file and adds "etc"
    return Path(__file__).resolve().parents[1]

def setup_logging(
    default_yaml: Path | None = None,
    default_conf: Path | None = None,
    env_var: str = "LOG_CFG",
) -> None:
    """
    Load logging from YAML (preferred) or INI, with robust path resolution.
    Order:
      1) $LOG_CFG if set
      2) <project_root>/etc/logging.yml (or .yaml)
      3) <project_root>/etc/logging.conf (INI)
    Falls back to basicConfig(INFO) if nothing found.
    """
    # pick default locations relative to project root
    root = _project_root()
    default_yaml = default_yaml or (root / "etc" / "logging.yml")
    default_conf = default_conf or (root / "etc" / "logging.conf")

    cfg_path = os.getenv(env_var)
    path = Path(cfg_path) if cfg_path else (
        default_yaml if default_yaml.exists() else default_conf
    )

    if not path or not path.exists():
        # last resort: quiet but usable defaults
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )
        logging.getLogger(__name__).warning("Logging config not found at %s", path)
        return

    # If using dictConfig to write to files, ensure directories exist
    def _ensure_log_dirs_for_dict(cfg: dict) -> None:
        for h in cfg.get("handlers", {}).values():
            fn = h.get("filename")
            if fn:
                Path(fn).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    if path.suffix.lower() in (".yml", ".yaml"):
        if yaml is None:
            raise RuntimeError("pyyaml not installed but YAML config requested")
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        # Be safe for repeated init
        cfg.setdefault("version", 1)
        cfg.setdefault("disable_existing_loggers", False)
        _ensure_log_dirs_for_dict(cfg)
        # Remove any preexisting basicConfig handlers so our config wins
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)
        logging.config.dictConfig(cfg)
    else:
        # INI/ConfigParser style
        # Ensure parent dirs exist for file handlers by reading the file first is harder;
        # rely on your INI to point to valid paths. Alternatively, create logs/ up front.
        logging.config.fileConfig(path, disable_existing_loggers=False)
