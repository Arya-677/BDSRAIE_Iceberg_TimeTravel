"""A small in-process SIMULATOR of the Iceberg features this project uses, built on plain Spark.

Purpose: let the orchestration logic (ingest, registry, train, retrain, rollback, retention, demo) run and be tested
where the real Iceberg runtime jar is unavailable. Enable with  TTML_ICEBERG=sim.

What it models: tables with immutable snapshots (parent chain, commit time), `table.snapshots` / `table.refs`
metadata queries, time-travel reads by snapshot id, append commits, tags (create/replace/drop), and the procedures
rollback_to_snapshot, expire_snapshots (older_than, retain_last, tag protection), rewrite_manifests.

What it does NOT model: file layout/bytes, partitioning, UPDATE/MERGE, delete files, metadata tables other than
snapshots/refs, concurrency, or any real Iceberg edge case. Passing here is NOT evidence that real Iceberg behaves
the same. Run the suite with TTML_ICEBERG=1 against the real runtime before relying on any result.
"""
from __future__ import annotations

import itertools
import re
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import _parse_datatype_string

from ttml.rows import frame_from_rows

_SIM = None
_ids = itertools.count(8_000_000_000_000_000_001, 7_919)


def _now():
    return datetime.now(timezone.utc)


class _Writer:
    def __init__(self, sim, name):
        self.sim, self.name = sim, name

    def append(self):
        self.sim._append(self.name, self.df)


def _install_writeTo():
    def writeTo(self, table):
        w = _Writer(_SIM, table)
        w.df = self
        return w
    DataFrame.writeTo = writeTo


class _Reader:
    def __init__(self, sim):
        self.sim, self._r, self._fmt, self._opts = sim, sim._real.read, None, {}

    def format(self, f):
        self._fmt = f
        return self

    def option(self, k, v):
        self._opts[k] = v
        self._r = self._r.option(k, v)
        return self

    def load(self, path=None, **kw):
        if self._fmt == "iceberg":
            return self.sim._read(path, self._opts.get("snapshot-id"))
        return self._r.format(self._fmt).load(path, **kw) if self._fmt else self._r.load(path, **kw)

    def __getattr__(self, n):          # csv(), parquet(), ...
        return getattr(self._r, n)


class SimSpark:
    is_sim = True

    def __init__(self, real: SparkSession):
        global _SIM
        self._real = real
        self.tables: dict[str, dict] = {}
        _SIM = self
        _install_writeTo()

    # ---- passthrough ----------------------------------------------------------------------------
    def __getattr__(self, n):
        return getattr(self._real, n)

    @property
    def read(self):
        return _Reader(self)

    def table(self, name):
        return self._head_df(name)

    # ---- internals ------------------------------------------------------------------------------
    def _t(self, name):
        if name not in self.tables:
            raise ValueError(f"[sim] table not found: {name}")
        return self.tables[name]

    def _head_df(self, name):
        t = self._t(name)
        return t["snaps"][t["head"]]["df"] if t["head"] is not None else frame_from_rows(self._real, [], t["schema"])

    def _read(self, name, snapshot_id):
        t = self._t(name)
        if snapshot_id is None:
            return self._head_df(name)
        sid = int(snapshot_id)
        if sid not in t["snaps"]:
            raise ValueError(f"[sim] Cannot find snapshot with ID {sid}")
        return t["snaps"][sid]["df"]

    def _append(self, name, df):
        t = self._t(name)
        df = df.select(*[df[f.name].cast(f.dataType).alias(f.name) for f in t["schema"].fields])    # by-name + type coercion
        cur = self._head_df(name) if t["head"] is not None else None
        new = df if cur is None else cur.unionByName(df)
        sid = next(_ids)
        t["snaps"][sid] = {"parent": t["head"], "ts": _now(), "df": new.localCheckpoint(eager=True), "op": "append"}
        t["head"] = sid

    def _refresh_views(self, name):
        t = self._t(name)
        v = name.replace(".", "_")
        rows = [(s["ts"], sid, s["parent"], s["op"]) for sid, s in sorted(t["snaps"].items(), key=lambda kv: kv[1]["ts"])]
        frame_from_rows(self._real, rows, "committed_at timestamp, snapshot_id bigint, parent_id bigint, operation string") \
            .createOrReplaceTempView(f"sim_snapshots_{v}")
        refs = [("main", "BRANCH", t["head"], None)] if t["head"] is not None else []
        refs += [(n, "TAG", x["sid"], x["retain_ms"]) for n, x in t["tags"].items()]
        frame_from_rows(self._real, refs, "name string, type string, snapshot_id bigint, max_reference_age_in_ms bigint") \
            .createOrReplaceTempView(f"sim_refs_{v}")

    @staticmethod
    def _cols_of(sql):
        i = sql.index("(")
        depth, j = 0, i
        while True:
            depth += sql[j] == "("
            depth -= sql[j] == ")"
            if depth == 0:
                break
            j += 1
        return sql[i + 1:j]

    def _ok(self, **row):
        return frame_from_rows(self._real, [tuple(row.values())] if row else [], ", ".join(f"{k} bigint" for k in row) or "x int")

    # ---- SQL dispatcher -------------------------------------------------------------------------
    def sql(self, q):
        s = " ".join(q.split())
        u = s.upper()
        if u.startswith("CREATE NAMESPACE"):
            return self._ok()
        m = re.match(r"CREATE TABLE IF NOT EXISTS (\S+) \(", s)
        if m:
            name = m.group(1)
            if name not in self.tables:
                schema = _parse_datatype_string(self._cols_of(q))
                self.tables[name] = {"schema": schema, "snaps": {}, "head": None, "tags": {}}
            return self._ok()
        m = re.match(r"DROP TABLE IF EXISTS (\S+)", s)
        if m:
            self.tables.pop(m.group(1), None)
            return self._ok()
        m = re.match(r"ALTER TABLE (\S+) (CREATE OR REPLACE|CREATE|REPLACE) TAG `([^`]+)`(?: AS OF VERSION (\d+))?(?: RETAIN (\d+) DAYS)?", s)
        if m:
            t = self._t(m.group(1))
            name, mode, tag = m.group(1), m.group(2), m.group(3)
            sid = int(m.group(4)) if m.group(4) else (t["tags"].get(tag, {}).get("sid"))
            if sid is None or sid not in t["snaps"]:
                raise ValueError(f"[sim] cannot tag: snapshot {sid} not found")
            if mode == "CREATE" and tag in t["tags"]:
                raise ValueError(f"[sim] tag {tag} already exists")
            if mode == "REPLACE" and tag not in t["tags"]:
                raise ValueError(f"[sim] tag {tag} does not exist")
            t["tags"][tag] = {"sid": sid, "retain_ms": int(m.group(5)) * 86_400_000 if m.group(5) else None}
            return self._ok()
        m = re.match(r"ALTER TABLE (\S+) DROP TAG `?([^`\s]+)`?", s)
        if m:
            self._t(m.group(1))["tags"].pop(m.group(2), None)
            return self._ok()
        m = re.match(r"CALL (\w+)\.system\.(\w+)\((.*)\)\s*$", s)
        if m:
            return self._call(m.group(1), m.group(2), m.group(3))
        # metadata queries: rewrite  <catalog.ns.table>.snapshots|refs  to the refreshed temp views
        def view(mm):
            self._refresh_views(mm.group(1))
            return f"sim_{mm.group(2)}_{mm.group(1).replace('.', '_')}"
        rewritten = re.sub(r"\b(\w+\.\w+\.\w+)\.(snapshots|refs)\b", view, q)
        if rewritten != q:
            return self._real.sql(rewritten)
        if re.match(r"(SELECT|WITH)\b", u) and not re.search(r"\bFROM\b", u.replace("WHERE 1 = 0", "")):
            return self._real.sql(q)              # table-free SELECT of literals (ttml.rows.frame_from_rows)
        raise NotImplementedError(f"[sim] unsupported SQL: {s[:120]}")

    def _call(self, cat, proc, args):
        a = args.strip()
        tbl = re.search(r"'([^']+)'", a).group(1)
        name = f"{cat}.{tbl}"
        t = self._t(name)
        if proc == "rollback_to_snapshot":
            sid = int(re.findall(r"(\d+)\s*$", a)[0])
            chain, x = [], t["head"]
            while x is not None:
                chain.append(x)
                x = t["snaps"].get(x, {}).get("parent")
            if sid not in chain:
                raise ValueError(f"[sim] Cannot roll back to snapshot, not an ancestor of the current state: {sid}")
            t["head"] = sid
            return self._ok(previous_current_snapshot_id=0, current_snapshot_id=sid)
        if proc == "expire_snapshots":
            older = re.search(r"older_than => TIMESTAMP '([^']+)'", a)
            keep_n = re.search(r"retain_last => (\d+)", a)
            cutoff = datetime.strptime(older.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) if older else None
            n = int(keep_n.group(1)) if keep_n else 1
            keep, x = set(), t["head"]
            for _ in range(n):                      # last n snapshots of main
                if x is None or x not in t["snaps"]:
                    break
                keep.add(x)
                x = t["snaps"][x]["parent"]
            keep |= {tg["sid"] for tg in t["tags"].values()}
            gone = [sid for sid, sn in t["snaps"].items() if sid not in keep and (cutoff is None or sn["ts"] < cutoff)]
            for sid in gone:
                del t["snaps"][sid]
            return self._ok(deleted_snapshots=len(gone))
        if proc in ("rewrite_manifests", "remove_orphan_files", "rewrite_data_files"):
            return self._ok(done=1)
        raise NotImplementedError(f"[sim] unsupported procedure {proc}")
