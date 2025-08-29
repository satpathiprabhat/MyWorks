"""
DB-API 2.0 wrapper over the FIS (GT.M) JDBC driver via JPype + JayDeBeApi.

Usage:
    import gtmdb_fis as gtm
    conn = gtm.connect(
        jdbc_url="jdbc:fisglobal:database=core;host=db-gw;port=6543;encrypt=com;poolsize=20",
        driver_jar="/opt/fis/fisglobal-driver.jar",                 # Windows: r"C:\path\to\driver.jar"
        driver_class="com.fisglobal.jdbc.Driver",                   # confirm from vendor jar/docs
        props={"user": "svc_user", "password": "******"},
        jvm_args=["-Xms128m", "-Xmx512m"],                          # optional
        classpath_extras=["/opt/fis/dep1.jar", "/opt/fis/dep2.jar"] # optional
    )
"""

from __future__ import annotations

import logging
import os
import platform
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple, List

import jpype
import jaydebeapi

log = logging.getLogger(__name__)


# ----------------------------- public API ------------------------------------


def connect(
    jdbc_url: str,
    driver_jar: str,
    driver_class: str,
    props: Optional[Dict[str, Any]] = None,
    jvm_path: Optional[str] = None,
    jvm_args: Optional[Sequence[str]] = None,
    classpath_extras: Optional[Sequence[str]] = None,
):
    """
    Return a DB-API 2.0 Connection backed by the vendor JDBC driver.

    Parameters
    ----------
    jdbc_url : str
        e.g. "jdbc:fisglobal:database=core;host=...;port=6543;..."
    driver_jar : str
        Path to the vendor JDBC jar.
    driver_class : str
        Fully-qualified JDBC driver class (from vendor docs/jar).
    props : dict | None
        Standard JDBC properties (e.g., {"user": "...", "password": "..."}).
    jvm_path : str | None
        Optional explicit JVM path (use if jpype.getDefaultJVMPath() fails).
    jvm_args : Sequence[str] | None
        Additional JVM flags, e.g. ["-Xmx512m"].
    classpath_extras : Sequence[str] | None
        Additional jar paths (dependencies). They will be added to the JVM classpath.

    Notes
    -----
    - JVM is started only once per process. Subsequent calls reuse it.
    - If the JVM is already started, any additional classpath jars cannot be added.
    """
    jars = _validate_and_collect_jars(driver_jar, classpath_extras or [])

    _ensure_jvm(
        classpath=jars,
        jvm_path=jvm_path,
        jvm_args=list(jvm_args or []),
    )

    # JayDeBeApi accepts str (single jar) or list (multiple)
    jar_arg = jars if len(jars) > 1 else jars[0]
    raw = jaydebeapi.connect(driver_class, jdbc_url, props or {}, jar_arg)
    return _Connection(raw)


# ----------------------------- internals -------------------------------------


def _validate_and_collect_jars(driver_jar: str, extras: Sequence[str]) -> List[str]:
    """
    Ensure all jar paths exist, return a normalized list of absolute jar paths.
    Raises FileNotFoundError with a clear message if any jar is missing.
    """
    all_paths = [driver_jar] + list(extras)
    normed: List[str] = []
    missing: List[str] = []

    for p in all_paths:
        # Expand ~ and environment variables (cross-platform)
        expanded = os.path.expandvars(os.path.expanduser(p))
        abs_path = os.path.abspath(expanded)
        if not os.path.isfile(abs_path):
            missing.append(p)
        else:
            normed.append(abs_path)

    if missing:
        raise FileNotFoundError(
            f"JAR not found: {missing}. "
            f"OS={platform.system()} cwd={os.getcwd()}"
        )

    return normed


def _ensure_jvm(classpath: Sequence[str], jvm_path: Optional[str], jvm_args: Sequence[str]) -> None:
    """
    Start the JVM if not already started, in a cross-platform safe way.
    Explicitly inject JPype's support jar (org.jpype.jar) to avoid
    'Can't find org.jpype.jar support library' on some systems.
    """
    if jpype.isJVMStarted():
        # JVM already running; can't extend classpath any further.
        if classpath:
            log.debug("JVM already started; classpath extras ignored on subsequent calls.")
        return

    # Locate JPype's support jar if present
    support_jar = None
    try:
        jpype_dir = os.path.dirname(jpype.__file__)
        cand = os.path.join(jpype_dir, "org.jpype.jar")
        if os.path.isfile(cand):
            support_jar = cand
    except Exception:
        pass

    # Build final classpath: [support_jar] + user jars (dedup, keep order)
    seen = set()
    jars: List[str] = []
    if support_jar and support_jar not in seen:
        jars.append(support_jar); seen.add(support_jar)
    for p in classpath:
        if p and p not in seen:
            jars.append(p); seen.add(p)

    # Guard against accidentally wiping the classpath via JVM args
    bad_args = [a for a in jvm_args if a.strip().startswith("-Djava.class.path=")]
    if bad_args:
        raise RuntimeError(
            "Do not pass -Djava.class.path in jvm_args; it overrides JPype's support jar. "
            f"Remove: {bad_args}"
        )

    # Resolve JVM path automatically if not supplied
    jvm = jvm_path or jpype.getDefaultJVMPath()

    # Optional diagnostics
    if os.environ.get("GTMDB_FIS_DEBUG", "0") not in ("", "0", "false", "False"):
        print("[GTMDB_FIS] Starting JVM")
        print("  OS         :", platform.platform())
        print("  Python     :", platform.python_version(), os.sys.executable)
        print("  JVM path   :", jvm)
        print("  JVM args   :", list(jvm_args))
        print("  Classpath  :")
        for j in jars:
            print("    -", j)
        try:
            print("  JPype dir  :", os.path.dirname(jpype.__file__))
        except Exception:
            pass

    # Start JVM with explicit classpath (keeps JPype support jar visible)
    jpype.startJVM(jvm, *jvm_args, classpath=jars)


# ----------------------------- DB-API shims ----------------------------------


class _Connection:
    """Thin DB-API connection wrapper to normalize behavior and add niceties."""

    def __init__(self, raw_conn):
        self._c = raw_conn
        self._closed = False

    # --- PEP-249 required methods
    def cursor(self, row_format: str | None = None):
        """
        Create a cursor. If row_format == 'dict', the fetch* methods will return dicts.
        (This is an optional convenience; default remains tuple rows per DB-API.)
        """
        c = _Cursor(self._c.cursor())
        if row_format == "dict":
            # monkey-patch tuple fetchers to dict fetchers transparently
            c.fetchone = c.fetchone_dict            # type: ignore[assignment]
            c.fetchmany = c.fetchmany_dict          # type: ignore[assignment]
            c.fetchall = c.fetchall_dict            # type: ignore[assignment]
        return c

    def commit(self):
        self._c.commit()

    def rollback(self):
        self._c.rollback()

    def close(self):
        if not self._closed:
            try:
                self._c.close()
            finally:
                self._closed = True

    # --- Optional conveniences
    @property
    def autocommit(self) -> bool:
        return bool(getattr(self._c.jconn, "getAutoCommit")())

    @autocommit.setter
    def autocommit(self, on: bool) -> None:
        getattr(self._c.jconn, "setAutoCommit")(bool(on))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc is not None and self.autocommit is False:
                self.rollback()
            elif self.autocommit is False:
                self.commit()
        finally:
            self.close()


class _Cursor:
    """PEP-249 minimal cursor shim with optional dict-style fetch helpers."""

    arraysize = 1

    def __init__(self, raw_cursor):
        self._cur = raw_cursor
        self._closed = False
        self._rowcount = -1
        self._description = None  # list of 7-item sequences per PEP-249

    # -------- PEP-249 attributes --------
    @property
    def description(self):
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    # -------- PEP-249 methods (tuples) --------
    def execute(self, operation: str, parameters=None):
        self._cur.execute(operation, list(parameters or []))
        self._rowcount = getattr(self._cur, "rowcount", -1)
        self._description = getattr(self._cur, "description", None)
        return self

    def executemany(self, operation: str, seq_of_parameters=None):
        self._cur.executemany(operation, [list(p) for p in (seq_of_parameters or [])])
        self._rowcount = getattr(self._cur, "rowcount", -1)
        self._description = getattr(self._cur, "description", None)
        return self

    def fetchone(self):
        r = self._cur.fetchone()
        return None if r is None else tuple(r)

    def fetchmany(self, size: int | None = None):
        n = size or self.arraysize
        rows = self._cur.fetchmany(n)
        return [tuple(r) for r in rows]

    def fetchall(self):
        rows = self._cur.fetchall()
        return [tuple(r) for r in rows]

    def close(self):
        if not self._closed:
            try:
                self._cur.close()
            finally:
                self._closed = True

    # -------- New: column names & dict-style fetch --------
    def columns(self) -> List[str]:
        """Return column names based on cursor.description (PEP-249)."""
        if not self._description:
            return []
        # Each description entry: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        return [d[0] for d in self._description]

    def fetchone_dict(self) -> Optional[Dict[str, Any]]:
        """Return next row as dict {col: value}, or None if no more rows."""
        row = self._cur.fetchone()
        if row is None:
            return None
        cols = self.columns()
        return dict(zip(cols, row))

    def fetchmany_dict(self, size: int | None = None) -> List[Dict[str, Any]]:
        """Return up to `size` rows as list of dicts."""
        n = size or self.arraysize
        rows = self._cur.fetchmany(n)
        cols = self.columns()
        return [dict(zip(cols, r)) for r in rows]

    def fetchall_dict(self) -> List[Dict[str, Any]]:
        """Return all remaining rows as list of dicts."""
        rows = self._cur.fetchall()
        cols = self.columns()
        return [dict(zip(cols, r)) for r in rows]
