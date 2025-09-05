"""
connector.py

DB-API-style wrapper for vendor JDBC drivers using JPype + JayDeBeApi.

Enhancements:
 - Auto-convert Python list/tuple in IN parameters to Java String[] (common ScJDBC pattern)
 - param_types support for IN typing hints
 - robust CallableStatement execution with execute/executeQuery fallback
 - dict/tuple fetch helpers
 - cross-platform JVM startup that keeps JPype support jar visible

Usage example (auto-conversion):
    res = conn.callproc(
        "xmrpc",
        in_params=["738", "1", ["APPID", "123456789"], None],
        out_param_types=[None, None, None, "VARCHAR"],
        result_as_dict=True
    )
"""

from __future__ import annotations

import logging
import os
import platform
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import jpype
import jaydebeapi
from jpype import JClass, JException, JString, JInt, JLong, JDouble, JBoolean, JArray, JByte

log = logging.getLogger(__name__)


# ---------- small mappings / helpers for SQL type -> getter or JPype creator ----------

SQLTYPE_TO_GETTER = {
    "VARCHAR": "getString",
    "CHAR": "getString",
    "LONGVARCHAR": "getString",
    "CLOB": "getString",
    "INTEGER": "getInt",
    "INT": "getInt",
    "SMALLINT": "getInt",
    "BIGINT": "getLong",
    "DECIMAL": "getBigDecimal",
    "NUMERIC": "getBigDecimal",
    "DOUBLE": "getDouble",
    "FLOAT": "getDouble",
    "REAL": "getDouble",
    "BOOLEAN": "getBoolean",
    "BIT": "getBoolean",
    "BINARY": "getBytes",
    "VARBINARY": "getBytes",
    "BLOB": "getBytes",
    "TIMESTAMP": "getTimestamp",
    "DATE": "getDate",
    "TIME": "getTime",
}


def _py_to_java_for_sql(val: Any, sql_type_name: Optional[str]):
    """Convert common Python values to Java objects appropriate for a SQL type."""
    if val is None:
        return None
    t = (sql_type_name or "").upper()
    try:
        if t in ("VARCHAR", "CHAR", "LONGVARCHAR", "CLOB", "DATE", "TIME", "TIMESTAMP"):
            return JString(str(val))
        if t in ("INTEGER", "INT", "SMALLINT"):
            return JInt(int(val))
        if t in ("BIGINT",):
            return JLong(int(val))
        if t in ("DOUBLE", "FLOAT", "REAL"):
            return JDouble(float(val))
        if t in ("BOOLEAN", "BIT"):
            return JBoolean(bool(val))
        if t in ("BINARY", "VARBINARY", "BLOB"):
            b = bytes(val) if not isinstance(val, (bytes, bytearray)) else val
            JByteArr = JArray(JByte)
            return JByteArr(b)
    except Exception:
        return JString(str(val))
    return JString(str(val))


def _java_to_python(jobj: Any) -> Any:
    """Convert common Java objects to Python-friendly types."""
    if jobj is None:
        return None
    try:
        if hasattr(jobj, "toString"):
            return str(jobj.toString())
    except Exception:
        pass
    return jobj


def _convert_py_sequence_to_jstring_array(seq: Sequence[Any]):
    """
    Convert a Python list/tuple to a Java String[] using JPype.

    Elements are converted via JString(str(element)).
    """
    JStr = JClass("java.lang.String")
    JStrArr = JArray(JStr)
    # Convert each element to Java String
    j_elems = [JString(str(x)) if x is not None else None for x in seq]
    return JStrArr(j_elems)


# ----------------------------- core connect & JVM startup -----------------------------

def connect(
    jdbc_url: str,
    driver_jar: str,
    driver_class: str,
    props: Optional[Dict[str, Any]] = None,
    jvm_path: Optional[str] = None,
    jvm_args: Optional[Sequence[str]] = None,
    classpath_extras: Optional[Sequence[str]] = None,
):
    jars = _validate_and_collect_jars(driver_jar, list(classpath_extras or []))
    _ensure_jvm(classpath=jars, jvm_path=jvm_path, jvm_args=list(jvm_args or []))
    jar_arg = jars if len(jars) > 1 else jars[0]
    raw = jaydebeapi.connect(driver_class, jdbc_url, props or {}, jar_arg)
    return _Connection(raw)


def _validate_and_collect_jars(driver_jar: str, extras: Sequence[str]) -> List[str]:
    all_paths = [driver_jar] + list(extras)
    normed: List[str] = []
    missing: List[str] = []
    for p in all_paths:
        expanded = os.path.expandvars(os.path.expanduser(p))
        try:
            abs_path = str(Path(expanded).resolve())
        except Exception:
            abs_path = expanded
        if not Path(abs_path).is_file():
            missing.append(p)
        else:
            normed.append(abs_path)
    if missing:
        raise FileNotFoundError(f"JAR(s) not found: {missing}. OS={platform.system()} cwd={os.getcwd()}")
    return normed


def _ensure_jvm(classpath: Sequence[str], jvm_path: Optional[str], jvm_args: Sequence[str]) -> None:
    if jpype.isJVMStarted():
        if classpath:
            log.debug("JVM already started; extra classpath entries ignored.")
        return

    support_jar = None
    try:
        jpype_dir = Path(jpype.__file__).parent
        cand = jpype_dir / "org.jpype.jar"
        if cand.exists():
            support_jar = str(cand.resolve())
    except Exception:
        support_jar = None

    seen = set()
    jars: List[str] = []
    if support_jar and support_jar not in seen:
        jars.append(support_jar); seen.add(support_jar)
    for p in classpath:
        if p and p not in seen:
            jars.append(p); seen.add(p)

    bad = [a for a in jvm_args if a.strip().startswith("-Djava.class.path=")]
    if bad:
        raise RuntimeError("Do not pass -Djava.class.path in jvm_args; JPype manages support jar.")

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
    jpype.startJVM(jvm, *jvm_args, classpath=jars)


# ----------------------------- DB-API shims (Connection & Cursor) -----------------------------

class _Connection:
    def __init__(self, raw_conn):
        self._c = raw_conn
        self._closed = False

    def cursor(self, row_format: Optional[str] = None):
        c = _Cursor(self._c.cursor())
        if row_format == "dict":
            c.fetchone = c.fetchone_dict  # type: ignore
            c.fetchmany = c.fetchmany_dict  # type: ignore
            c.fetchall = c.fetchall_dict  # type: ignore
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
            if exc_type and not self.autocommit:
                self.rollback()
            elif not self.autocommit:
                self.commit()
        finally:
            self.close()

    # ---------------- callproc with auto-conversion of Python sequences to Java String[] ----------------
    def callproc(
        self,
        proc_name: str,
        in_params: Optional[Sequence[Any]] = None,
        out_param_types: Optional[Sequence[Optional[str]]] = None,
        param_types: Optional[Sequence[Optional[str]]] = None,
        result_as_dict: bool = False,
    ) -> Dict[str, Any]:
        """
        Call a stored procedure using JDBC CallableStatement.

        - If an element of in_params is a Python list/tuple, it will be converted
          into a Java String[] automatically (common ScJDBC pattern).
        - param_types: optional SQL type hints for IN params (e.g. ["INTEGER", None, "VARCHAR"])
        """
        raw_jconn = getattr(self._c, "jconn", None)
        if raw_jconn is None:
            raise RuntimeError("Underlying JDBC 'jconn' not available")

        in_params = list(in_params or [])
        out_types = list(out_param_types or [])
        param_types = list(param_types or [])

        # Auto-convert Python sequences found in in_params into Java String[] objects
        for i, v in enumerate(in_params):
            if isinstance(v, (list, tuple)):
                if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
                    print(f"[callproc] auto-converting in_params[{i}] (Python sequence) -> Java String[]")
                in_params[i] = _convert_py_sequence_to_jstring_array(v)

        total_params = max(len(in_params), len(out_types), 0)
        placeholders = ",".join(["?"] * total_params) if total_params > 0 else ""
        call_syntax = f"{{call {proc_name}({placeholders})}}" if placeholders else f"{{call {proc_name}()}}"

        if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
            print("[callproc] call_syntax:", call_syntax)
            print("[callproc] in_params:", in_params)
            print("[callproc] out_types:", out_types)
            print("[callproc] param_types:", param_types)

        cstmt = raw_jconn.prepareCall(call_syntax)
        SQLTypes = JClass("java.sql.Types")

        # Set IN params (type-aware) and register OUT params
        for i in range(total_params):
            idx = i + 1

            # Determine IN SQL type hint for marshalling
            in_sql_type = None
            if i < len(param_types) and param_types[i]:
                in_sql_type = param_types[i].upper()
            elif i < len(out_types) and out_types[i]:
                in_sql_type = out_types[i].upper()
            else:
                in_sql_type = "VARCHAR"

            # Set IN param if present
            if i < len(in_params):
                raw_val = in_params[i]
                # If the raw_val is a JPype java array/object already, set directly
                if hasattr(raw_val, "__javaclass__") or isinstance(raw_val, (JClass,)):
                    try:
                        cstmt.setObject(idx, raw_val)
                        if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
                            print(f"[callproc] set IN idx={idx} using JPype java object (auto-converted)")
                    except Exception as ex:
                        if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
                            print(f"[callproc] failed setObject for JPype object idx={idx}: {ex}")
                        cstmt.setObject(idx, JString(str(raw_val)))
                else:
                    # Normal marshalling
                    jval = _py_to_java_for_sql(raw_val, in_sql_type)
                    if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
                        print(f"[callproc] set IN idx={idx} sqltype={in_sql_type} pyval={raw_val} jval_type={type(jval)}")
                    try:
                        if in_sql_type in ("INTEGER", "INT", "SMALLINT"):
                            cstmt.setInt(idx, int(raw_val) if raw_val is not None else None)
                        elif in_sql_type == "BIGINT":
                            cstmt.setLong(idx, int(raw_val) if raw_val is not None else None)
                        elif in_sql_type in ("DOUBLE", "FLOAT", "REAL"):
                            cstmt.setDouble(idx, float(raw_val) if raw_val is not None else None)
                        elif in_sql_type in ("BOOLEAN", "BIT"):
                            cstmt.setBoolean(idx, bool(raw_val))
                        else:
                            cstmt.setObject(idx, jval)
                    except Exception as ex:
                        if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
                            print(f"[callproc] typed set failed idx={idx}: {ex}; falling back to setObject(str)")
                        try:
                            cstmt.setObject(idx, JString(str(raw_val)) if raw_val is not None else None)
                        except Exception:
                            cstmt.setObject(idx, None)

            # Register OUT param if needed
            if i < len(out_types) and out_types[i]:
                tname = out_types[i].upper()
                if not hasattr(SQLTypes, tname):
                    raise ValueError(f"Unknown java.sql.Types name: {tname}")
                jtype = getattr(SQLTypes, tname)
                if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
                    print(f"[callproc] register OUT idx={idx} type={tname}")
                cstmt.registerOutParameter(idx, jtype)

        # Execute robustly
        try:
            rs_obj, update_count = _try_execute_callable(cstmt)
        except Exception:
            try: cstmt.close()
            except Exception: pass
            raise

        # Convert result rows
        rows: List[Any] = []
        if rs_obj is not None:
            rows = _resultset_to_python(rs_obj, as_dict=result_as_dict)
        elif update_count is not None:
            rows = [("UPDATE_COUNT", int(update_count))]

        # Read OUT params with type-specific getters
        out_values: List[Any] = []
        for i in range(total_params):
            if i < len(out_types) and out_types[i]:
                tname = out_types[i].upper()
                getter = SQLTYPE_TO_GETTER.get(tname)
                idx = i + 1
                try:
                    if getter and hasattr(cstmt, getter):
                        val = getattr(cstmt, getter)(idx)
                    else:
                        val = cstmt.getObject(idx)
                    out_values.append(_java_to_python(val))
                except Exception as ex:
                    if os.getenv("GTMDB_FIS_DEBUG", "0").lower() in ("1", "true", "yes"):
                        print(f"[callproc] OUT getter failed idx={idx} type={tname}: {ex}")
                    try:
                        out_values.append(_java_to_python(cstmt.getObject(idx)))
                    except Exception:
                        out_values.append(None)
            else:
                out_values.append(None)

        try: cstmt.close()
        except Exception: pass

        return {"out_params": out_values, "result_rows": rows}


class _Cursor:
    arraysize = 1

    def __init__(self, raw_cursor):
        self._cur = raw_cursor
        self._closed = False
        self._rowcount = -1
        self._description = None

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

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
            try: self._cur.close()
            finally: self._closed = True

    # dict helpers
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


# ----------------------------- Helpers for callable execution -----------------------------

def _resultset_to_python(rs, as_dict: bool = False) -> List[Any]:
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
    try: rs.close()
    except Exception: pass
    return rows


def _try_execute_callable(cstmt):
    """
    Execute a CallableStatement robustly.
    Returns (resultset_obj_or_None, update_count_or_None)
    """
    try:
        has_rs = cstmt.execute()
    except JException as ex:
        msg = str(ex)
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
        raise

    if has_rs:
        try:
            rs = cstmt.getResultSet()
            return rs, None
        except Exception:
            return None, None

    try:
        update_count = cstmt.getUpdateCount()
        if update_count is not None and int(update_count) != -1:
            return None, update_count
    except Exception:
        pass

    try:
        rs = cstmt.executeQuery()
        return rs, None
    except Exception:
        return None, None
