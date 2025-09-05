"""
DB-API 2.0 wrapper over the FIS (GT.M) JDBC driver via JPype + JayDeBeApi.

Adds:
  - Safe JVM startup across OS
  - Tuple & dict fetch helpers
  - callproc() for stored procedures (CallableStatement support)

Usage:
    import gtmdb_fis as gtm
    conn = gtm.connect(
        jdbc_url="jdbc:fisglobal:database=core;host=db-gw;port=6543;encrypt=com",
        driver_jar="/opt/fis/fisglobal-driver.jar",   # Windows: r"C:\path\to\driver.jar"
        driver_class="com.fisglobal.jdbc.Driver",
        props={"user": "svc_user", "password": "******"},
    )

    res = conn.callproc("xmrpc",
                        in_params=[777, 1, "MB", None],
                        out_param_types=[None, None, None, "VARCHAR"],
                        result_as_dict=True)
    print(res["out_params"], res["result_rows"])
"""

from __future__ import annotations

import logging
import os
import platform
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple, List

import jpype
import jaydebeapi
from jpype import JClass

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
    jars = _validate_and_collect_jars(driver_jar, classpath_extras or [])
    _ensure_jvm(classpath=jars, jvm_path=jvm_path, jvm_args=list(jvm_args or []))
    jar_arg = jars if len(jars) > 1 else jars[0]
    raw = jaydebeapi.connect(driver_class, jdbc_url, props or {}, jar_arg)
    return _Connection(raw)


# ----------------------------- internals -------------------------------------


def _validate_and_collect_jars(driver_jar: str, extras: Sequence[str]) -> List[str]:
    all_paths = [driver_jar] + list(extras)
    normed: List[str] = []
    missing: List[str] = []
    for p in all_paths:
        expanded = os.path.expandvars(os.path.expanduser(p))
        abs_path = os.path.abspath(expanded)
        if not os.path.isfile(abs_path):
            missing.append(p)
        else:
            normed.append(abs_path)
    if missing:
        raise FileNotFoundError(
            f"JAR not found: {missing}. OS={platform.system()} cwd={os.getcwd()}"
        )
    return normed


def _ensure_jvm(classpath: Sequence[str], jvm_path: Optional[str], jvm_args: Sequence[str]) -> None:
    if jpype.isJVMStarted():
        if classpath:
            log.debug("JVM already started; classpath extras ignored.")
        return

    support_jar = None
    try:
        jpype_dir = os.path.dirname(jpype.__file__)
        cand = os.path.join(jpype_dir, "org.jpype.jar")
        if os.path.isfile(cand):
            support_jar = cand
    except Exception:
        pass

    seen = set()
    jars: List[str] = []
    if support_jar and support_jar not in seen:
        jars.append(support_jar); seen.add(support_jar)
    for p in classpath:
        if p and p not in seen:
            jars.append(p); seen.add(p)

    bad_args = [a for a in jvm_args if a.strip().startswith("-Djava.class.path=")]
    if bad_args:
        raise RuntimeError("Remove -Djava.class.path from jvm_args; JPype manages classpath.")

    jvm = jvm_path or jpype.getDefaultJVMPath()
    if os.environ.get("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
        print("[GTMDB_FIS] Starting JVM")
        print("  JVM path   :", jvm)
        print("  JVM args   :", list(jvm_args))
        print("  Classpath  :")
        for j in jars:
            print("    -", j)
    jpype.startJVM(jvm, *jvm_args, classpath=jars)


# ----------------------------- DB-API shims ----------------------------------


class _Connection:
    def __init__(self, raw_conn):
        self._c = raw_conn
        self._closed = False

    def cursor(self, row_format: str | None = None):
        c = _Cursor(self._c.cursor())
        if row_format == "dict":
            c.fetchone = c.fetchone_dict          # type: ignore
            c.fetchmany = c.fetchmany_dict        # type: ignore
            c.fetchall = c.fetchall_dict          # type: ignore
        return c

    def commit(self): self._c.commit()
    def rollback(self): self._c.rollback()

    def close(self):
        if not self._closed:
            try: self._c.close()
            finally: self._closed = True

    @property
    def autocommit(self) -> bool:
        return bool(getattr(self._c.jconn, "getAutoCommit")())
    @autocommit.setter
    def autocommit(self, on: bool) -> None:
        getattr(self._c.jconn, "setAutoCommit")(bool(on))

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc and not self.autocommit: self.rollback()
            elif not self.autocommit: self.commit()
        finally: self.close()

    # -------- CallableStatement support --------
    def callproc(
        self,
        proc_name: str,
        in_params: Optional[Sequence[Any]] = None,
        out_param_types: Optional[Sequence[Optional[str]]] = None,
        result_as_dict: bool = False,
    ) -> dict:
        raw_conn = getattr(self._c, "jconn", None)
        if raw_conn is None:
            raise RuntimeError("Underlying JDBC jconn not available")
        in_params = list(in_params or [])
        out_types = list(out_param_types or [])
        total_params = max(len(in_params), len(out_types))
        placeholders = ",".join(["?"] * total_params)
        call_syntax = f"{{call {proc_name}({placeholders})}}"
        cstmt = raw_conn.prepareCall(call_syntax)
        SQLTypes = JClass("java.sql.Types")
        # set IN + register OUT
        for i in range(total_params):
            idx = i + 1
            if i < len(in_params):
                try: cstmt.setObject(idx, in_params[i])
                except Exception: cstmt.setObject(idx, str(in_params[i]))
            if i < len(out_types) and out_types[i]:
                tname = out_types[i].upper()
                if not hasattr(SQLTypes, tname):
                    raise ValueError(f"Unknown java.sql.Types name: {tname}")
                cstmt.registerOutParameter(idx, getattr(SQLTypes, tname))
        has_rs = cstmt.execute()
        rows = []
        if has_rs: rows = _resultset_to_python(cstmt.getResultSet(), as_dict=result_as_dict)
        out_vals = []
        for i in range(total_params):
            if i < len(out_types) and out_types[i]:
                try: out_vals.append(_java_to_python(cstmt.getObject(i + 1)))
                except Exception: out_vals.append(None)
            else:
                out_vals.append(None)
        try: cstmt.close()
        except Exception: pass
        return {"out_params": out_vals, "result_rows": rows}


class _Cursor:
    arraysize = 1
    def __init__(self, raw_cursor):
        self._cur = raw_cursor; self._closed = False
        self._rowcount = -1; self._description = None
    @property
    def description(self): return self._description
    @property
    def rowcount(self): return self._rowcount
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
        r = self._cur.fetchone(); return None if r is None else tuple(r)
    def fetchmany(self, size=None):
        n = size or self.arraysize; rows = self._cur.fetchmany(n)
        return [tuple(r) for r in rows]
    def fetchall(self):
        rows = self._cur.fetchall(); return [tuple(r) for r in rows]
    def close(self):
        if not self._closed:
            try: self._cur.close()
            finally: self._closed = True
    # --- dict helpers ---
    def columns(self): return [d[0] for d in self._description] if self._description else []
    def fetchone_dict(self): 
        r = self._cur.fetchone(); return None if r is None else dict(zip(self.columns(), r))
    def fetchmany_dict(self, size=None):
        n = size or self.arraysize; rows = self._cur.fetchmany(n)
        return [dict(zip(self.columns(), r)) for r in rows]
    def fetchall_dict(self):
        rows = self._cur.fetchall(); return [dict(zip(self.columns(), r)) for r in rows]


# ----------------------------- helpers ----------------------------------


def _java_to_python(jobj):
    if jobj is None: return None
    try:
        if hasattr(jobj, "toString"): return str(jobj.toString())
    except Exception: pass
    return jobj


def _resultset_to_python(rs, as_dict=False):
    rows = []
    if rs is None: return rows
    md = rs.getMetaData(); col_count = md.getColumnCount()
    col_names = [md.getColumnLabel(i) or md.getColumnName(i) for i in range(1, col_count + 1)]
    while rs.next():
        vals = [_java_to_python(rs.getObject(i)) for i in range(1, col_count + 1)]
        rows.append(dict(zip(col_names, vals)) if as_dict else tuple(vals))
    try: rs.close()
    except Exception: pass
    return rows
