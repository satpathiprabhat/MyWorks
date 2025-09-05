"""
connector.py

DB-API-style wrapper for vendor JDBC drivers using JPype + JayDeBeApi.

Features:
 - Cross-platform JVM startup that preserves JPype's support jar
 - Validation of driver JARs
 - Cursor with tuple/dict fetch helpers
 - callproc(...) using JDBC CallableStatement with fallback to executeQuery()/executeUpdate()
 - Helper conversions from Java objects to Python-friendly types

Usage:
    conn = connect(
        jdbc_url="jdbc:fisglobal:database=core;host=...;port=6543",
        driver_jar="/path/to/ScJDBC-3.8.10.1.jar",
        driver_class="com.fisglobal.jdbc.Driver",
        props={"user": "svc", "password": "xxx"},
        jvm_path=r"C:\Program Files\Java\jdk-17\bin\server\jvm.dll"  # optional if JPype cannot find JVM
    )

    # SQL usage (JayDeBeApi)
    cur = conn.cursor(row_format="dict")
    cur.execute("SELECT 1")
    print(cur.fetchall())

    # CALLABLE usage
    result = conn.callproc(
        "xmrpc",
        in_params=[777, 1, "MB", None],
        out_param_types=[None, None, None, "VARCHAR"],
        result_as_dict=True,
    )
    print(result)
"""

from __future__ import annotations

import logging
import os
import platform
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import jpype
import jaydebeapi
from jpype import JClass, JException

log = logging.getLogger(__name__)


# ----------------------------- Public API -----------------------------------


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
    Create and return a DB-API like connection wrapper.

    - jdbc_url: JDBC connection URL for the driver
    - driver_jar: path to primary vendor jar
    - driver_class: fully qualified driver class name
    - props: dict of JDBC properties (user/password/etc.)
    - jvm_path: explicit path to jvm library (optional)
    - jvm_args: list of JVM args, e.g. ["-Xmx512m"]
    - classpath_extras: list of additional jars required by the driver
    """
    jars = _validate_and_collect_jars(driver_jar, list(classpath_extras or []))
    _ensure_jvm(classpath=jars, jvm_path=jvm_path, jvm_args=list(jvm_args or []))

    # JayDeBeApi accepts a single jar path (str) or list of jar paths
    jar_arg = jars if len(jars) > 1 else jars[0]
    raw = jaydebeapi.connect(driver_class, jdbc_url, props or {}, jar_arg)
    return _Connection(raw)


# ----------------------------- Internals ------------------------------------


def _validate_and_collect_jars(driver_jar: str, extras: Sequence[str]) -> List[str]:
    """
    Normalize and verify jar paths. Return absolute list of jar paths.
    Raise FileNotFoundError with clear message if missing.
    """
    all_paths = [driver_jar] + list(extras)
    normed: List[str] = []
    missing: List[str] = []

    for p in all_paths:
        expanded = os.path.expandvars(os.path.expanduser(p))
        abs_path = str(Path(expanded).resolve())
        if not Path(abs_path).is_file():
            missing.append(p)
        else:
            normed.append(abs_path)

    if missing:
        raise FileNotFoundError(
            f"JAR(s) not found: {missing}. OS={platform.system()} cwd={os.getcwd()}"
        )
    return normed


def _ensure_jvm(classpath: Sequence[str], jvm_path: Optional[str], jvm_args: Sequence[str]) -> None:
    """
    Start the JVM safely:

    - Avoid overriding JPype support jar.
    - Explicitly include JPype support jar if present.
    - Use jpype.startJVM(..., classpath=jars) rather than -Djava.class.path=...
    """
    if jpype.isJVMStarted():
        if classpath:
            log.debug("JVM already started; additional classpath entries will be ignored.")
        return

    # Find JPype support jar (org.jpype.jar) if available next to module
    support_jar = None
    try:
        jpype_dir = Path(jpype.__file__).parent
        cand = jpype_dir / "org.jpype.jar"
        if cand.exists():
            support_jar = str(cand.resolve())
    except Exception:
        support_jar = None

    # Final classpath: support jar first (if found), then user jars (dedup preserve order)
    seen = set()
    jars: List[str] = []
    if support_jar and support_jar not in seen:
        jars.append(support_jar)
        seen.add(support_jar)
    for p in classpath:
        if p and p not in seen:
            jars.append(p)
            seen.add(p)

    # Prevent user from accidentally overriding classpath via jvm_args
    bad = [a for a in jvm_args if a.strip().startswith("-Djava.class.path=")]
    if bad:
        raise RuntimeError("Do not pass -Djava.class.path in jvm_args; it would hide JPype's support jar.")

    jvm = jvm_path or jpype.getDefaultJVMPath()
    debug_on = os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes")
    if debug_on:
        print("[GTMDB_FIS] Starting JVM")
        print("  OS:", platform.platform())
        print("  JVM path:", jvm)
        print("  JVM args:", list(jvm_args))
        print("  Classpath entries:")
        for x in jars:
            print("    -", x)
        try:
            print("  JPype dir:", Path(jpype.__file__).parent)
        except Exception:
            pass

    jpype.startJVM(jvm, *jvm_args, classpath=jars)


# ----------------------------- DB-API shims ---------------------------------


class _Connection:
    """Thin wrapper that exposes commit/rollback/close and callproc support."""

    def __init__(self, raw_conn):
        # raw_conn is jaydebeapi connection object
        self._c = raw_conn
        self._closed = False

    def cursor(self, row_format: Optional[str] = None):
        c = _Cursor(self._c.cursor())
        if row_format == "dict":
            # monkey-patch standard fetchers to dict versions
            c.fetchone = c.fetchone_dict  # type: ignore
            c.fetchmany = c.fetchmany_dict  # type: ignore
            c.fetchall = c.fetchall_dict  # type: ignore
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
            if exc_type and not self.autocommit:
                self.rollback()
            elif not self.autocommit:
                self.commit()
        finally:
            self.close()

    # ---------------- CallableStatement support ----------------
    def callproc(
        self,
        proc_name: str,
        in_params: Optional[Sequence[Any]] = None,
        out_param_types: Optional[Sequence[Optional[str]]] = None,
        result_as_dict: bool = False,
    ) -> Dict[str, Any]:
        """
        Call a stored procedure using JDBC CallableStatement.

        - proc_name: name of the stored procedure (no surrounding braces)
        - in_params: list of IN parameter values (put None for OUT placeholders if needed)
        - out_param_types: list aligned to parameter positions with java.sql.Types names (e.g. "VARCHAR")
                           use None for positions that are not OUT params
        - result_as_dict: if True, convert resultset rows to dicts (colname -> value)

        Returns: {"out_params": [...], "result_rows": [...]}
        """
        # Extract underlying java.sql.Connection exposed by jaydebeapi as .jconn
        raw_jconn = getattr(self._c, "jconn", None)
        if raw_jconn is None:
            raise RuntimeError("Underlying JDBC 'jconn' not available - cannot perform CallableStatement calls")

        in_params = list(in_params or [])
        out_types = list(out_param_types or [])
        total_params = max(len(in_params), len(out_types), 0)
        placeholders = ",".join(["?"] * total_params) if total_params > 0 else ""
        call_syntax = f"{{call {proc_name}({placeholders})}}" if placeholders else f"{{call {proc_name}()}}"

        # Prepare the callable
        cstmt = raw_jconn.prepareCall(call_syntax)
        SQLTypes = JClass("java.sql.Types")

        # Set IN params and register OUT params
        for i in range(total_params):
            idx = i + 1
            if i < len(in_params):
                val = in_params[i]
                try:
                    cstmt.setObject(idx, val)
                except Exception:
                    # fallback to string representation
                    cstmt.setObject(idx, str(val) if val is not None else None)
            if i < len(out_types) and out_types[i]:
                tname = out_types[i].upper()
                if not hasattr(SQLTypes, tname):
                    raise ValueError(f"Unknown java.sql.Types name: {tname}")
                jtype = getattr(SQLTypes, tname)
                cstmt.registerOutParameter(idx, jtype)

        # Execute in a robust fashion (drivers sometimes demand executeQuery or executeUpdate)
        try:
            rs_obj, update_count = _try_execute_callable(cstmt)
        except Exception as exc:
            try:
                cstmt.close()
            except Exception:
                pass
            raise

        # Convert resultset (if any)
        rows: List[Any] = []
        if rs_obj is not None:
            rows = _resultset_to_python(rs_obj, as_dict=result_as_dict)
        elif update_count is not None:
            rows = [("UPDATE_COUNT", int(update_count))]

        # Collect OUT params
        out_values: List[Any] = []
        for i in range(total_params):
            if i < len(out_types) and out_types[i]:
                try:
                    val = cstmt.getObject(i + 1)
                    out_values.append(_java_to_python(val))
                except Exception:
                    out_values.append(None)
            else:
                out_values.append(None)

        # Close statement
        try:
            cstmt.close()
        except Exception:
            pass

        return {"out_params": out_values, "result_rows": rows}


class _Cursor:
    """Minimal DB-API cursor wrapper with dict helpers."""

    arraysize = 1

    def __init__(self, raw_cursor):
        self._cur = raw_cursor
        self._closed = False
        self._rowcount = -1
        self._description = None  # PEP-249: sequence of 7-item tuples

    # PEP-249 attributes
    @property
    def description(self):
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    # PEP-249 methods (tuple-based)
    def execute(self, operation: str, parameters: Optional[Iterable[Any]] = None):
        self._cur.execute(operation, list(parameters or []))
        self._rowcount = getattr(self._cur, "rowcount", -1)
        self._description = getattr(self._cur, "description", None)
        return self

    def executemany(self, operation: str, seq_of_parameters: Iterable[Iterable[Any]]):
        self._cur.executemany(operation, [list(p) for p in seq_of_parameters])
        self._rowcount = getattr(self._cur, "rowcount", -1)
        self._description = getattr(self._cur, "description", None)
        return self

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        r = self._cur.fetchone()
        return None if r is None else tuple(r)

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        n = size or self.arraysize
        rows = self._cur.fetchmany(n)
        return [tuple(r) for r in rows]

    def fetchall(self) -> List[Tuple[Any, ...]]:
        rows = self._cur.fetchall()
        return [tuple(r) for r in rows]

    def close(self):
        if not self._closed:
            try:
                self._cur.close()
            finally:
                self._closed = True

    # Dict-style helpers
    def columns(self) -> List[str]:
        if not self._description:
            return []
        return [d[0] for d in self._description]

    def fetchone_dict(self) -> Optional[Dict[str, Any]]:
        r = self._cur.fetchone()
        if r is None:
            return None
        return dict(zip(self.columns(), r))

    def fetchmany_dict(self, size: Optional[int] = None) -> List[Dict[str, Any]]:
        n = size or self.arraysize
        rows = self._cur.fetchmany(n)
        return [dict(zip(self.columns(), r)) for r in rows]

    def fetchall_dict(self) -> List[Dict[str, Any]]:
        rows = self._cur.fetchall()
        return [dict(zip(self.columns(), r)) for r in rows]


# ----------------------------- Helper functions -----------------------------


def _java_to_python(jobj: Any) -> Any:
    """
    Convert certain Java objects to Python-friendly types.
    - Strings and primitives are usually auto-mapped by JPype.
    - For java.sql.Timestamp/Date we return ISO string (safe).
    - Fallback: str(jobj).
    """
    if jobj is None:
        return None
    try:
        # java.sql.Timestamp, java.sql.Date typically have toString()
        if hasattr(jobj, "toString"):
            return str(jobj.toString())
    except Exception:
        pass
    # If JPype already mapped it to a Python primitive, return as-is
    return jobj


def _resultset_to_python(rs, as_dict: bool = False) -> List[Any]:
    """Convert java.sql.ResultSet to list of Python tuples or dicts."""
    rows: List[Any] = []
    if rs is None:
        return rows
    md = rs.getMetaData()
    col_count = md.getColumnCount()
    col_names = [md.getColumnLabel(i) or md.getColumnName(i) for i in range(1, col_count + 1)]
    while rs.next():
        vals = []
        for i in range(1, col_count + 1):
            try:
                v = rs.getObject(i)
            except Exception:
                v = None
            vals.append(_java_to_python(v))
        rows.append(dict(zip(col_names, vals)) if as_dict else tuple(vals))
    try:
        rs.close()
    except Exception:
        pass
    return rows


def _try_execute_callable(cstmt):
    """
    Try to execute CallableStatement, handling drivers that require executeQuery() or executeUpdate().

    Returns: (resultset_obj_or_None, update_count_or_None)
    """
    try:
        has_rs = cstmt.execute()
    except JException as ex:
        # Inspect message to detect driver guidance
        msg = str(ex)
        # If driver instructs executeQuery()
        if "executeQuery" in msg or "executequery" in msg.lower():
            try:
                rs = cstmt.executeQuery()
                return rs, None
            except Exception as e:
                raise RuntimeError("CallableStatement.executeQuery() failed") from e
        if "executeUpdate" in msg or "executeupdate" in msg.lower():
            try:
                upd = cstmt.executeUpdate()
                return None, upd
            except Exception as e:
                raise RuntimeError("CallableStatement.executeUpdate() failed") from e
        # Unknown SQL exception - rethrow
        raise

    # If execute returned True -> there's a ResultSet
    if has_rs:
        try:
            rs = cstmt.getResultSet()
            return rs, None
        except Exception:
            return None, None

    # execute returned False -> check update count
    try:
        update_count = cstmt.getUpdateCount()
        if update_count is not None and int(update_count) != -1:
            return None, update_count
    except Exception:
        pass

    # As last resort some drivers still need executeQuery even if execute returned False
    try:
        rs = cstmt.executeQuery()
        return rs, None
    except Exception:
        return None, None
