#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
topicfoundry — Forge event schemas & filters from ABIs (offline).

What you get (offline):
  • topic0: keccak("EventName(type1,type2,...)")
  • Indexed vs data columns with normalized on-chain types
  • SQL DDL: PostgreSQL, BigQuery, ClickHouse (one table per event)
  • JSON Schema per event + global manifest
  • CSV data dictionary (event, param, type, indexed, position)
  • eth_getLogs filter stubs (topics[] arrays)

Examples:
  $ python topicfoundry.py build ./abis/*.json --pretty
  $ python topicfoundry.py ddl ./abis/*.json --target postgres > schema.sql
  $ python topicfoundry.py json ./abis/*.json --out schemas.json
  $ python topicfoundry.py dict ./abis/*.json --out dict.csv
  $ python topicfoundry.py filters ./abis/*.json --pretty
"""

import glob
import json
import os
import sys
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import click
from eth_utils import keccak

# ------------------------------ helpers ------------------------------

def load_abi_any(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("abi"), list):
            return data["abi"]
        if "result" in data:
            try:
                arr = json.loads(data["result"])
                if isinstance(arr, list):
                    return arr
            except Exception:
                pass
    raise click.ClickException(f"Unrecognized ABI format: {path}")

def normalize_type(t: str) -> str:
    # Canonicalize common Solidity shorthands
    if t == "uint": return "uint256"
    if t == "int": return "int256"
    return t

def event_signature(name: str, inputs: List[Dict[str, Any]]) -> str:
    types = ",".join(normalize_type(i.get("type","")) for i in inputs)
    return f"{name}({types})"

def topic0(sig: str) -> str:
    return "0x" + keccak(text=sig).hex()

# SQL type mappings (lossless & pragmatic)
PG_TYPES = {
    "address": "bytea",       # 20 bytes
    "bool": "boolean",
    "bytes32": "bytea",
    "bytes": "bytea",
    "string": "text",
    "uint256": "numeric(78)",
    "int256": "numeric(78)",
}
BQ_TYPES = {
    "address": "BYTES",
    "bool": "BOOL",
    "bytes32": "BYTES",
    "bytes": "BYTES",
    "string": "STRING",
    "uint256": "NUMERIC",     # 38 digits; use BIGNUMERIC when needed
    "int256": "NUMERIC",
}
CH_TYPES = {
    "address": "FixedString(20)",
    "bool": "UInt8",
    "bytes32": "FixedString(32)",
    "bytes": "String",
    "string": "String",
    "uint256": "Decimal(76,0)",
    "int256": "Decimal(76,0)",
}

def to_sql_type(sol: str, target: str) -> str:
    sol = normalize_type(sol)
    base = sol
    if sol.endswith("[]"):
        # arrays: store as JSON for portability
        return "jsonb" if target == "postgres" else ("JSON" if target == "bigquery" else "JSON")
    if target == "postgres":
        return PG_TYPES.get(base, "text")
    if target == "bigquery":
        return ("BIGNUMERIC" if base in ("uint256","int256") else BQ_TYPES.get(base, "STRING"))
    if target == "clickhouse":
        return CH_TYPES.get(base, "String")
    return "text"

# ------------------------------ models ------------------------------

@dataclass
class Param:
    name: str
    type: str
    indexed: bool
    position: int

@dataclass
class EventModel:
    file: str
    contract: str
    name: str
    anonymous: bool
    signature: str
    topic0: str
    inputs: List[Param]

# ------------------------------ extraction ------------------------------

def extract_events(path: str) -> List[EventModel]:
    abi = load_abi_any(path)
    out: List[EventModel] = []
    contract_name = os.path.splitext(os.path.basename(path))[0]
    for it in abi:
        if it.get("type") != "event":
            continue
        name = it.get("name", "")
        ins = it.get("inputs", [])
        sig = event_signature(name, ins)
        t0 = topic0(sig)
        params = []
        for i, p in enumerate(ins):
            params.append(Param(
                name=p.get("name") or f"arg{i}",
                type=normalize_type(p.get("type","")),
                indexed=bool(p.get("indexed", False)),
                position=i
            ))
        out.append(EventModel(
            file=os.path.basename(path), contract=contract_name, name=name,
            anonymous=bool(it.get("anonymous", False)),
            signature=sig, topic0=t0, inputs=params
        ))
    return out

def expand_paths(paths: List[str]) -> List[str]:
    out: List[str] = []
    for p in paths:
        g = glob.glob(p)
        if g:
            out.extend(g)
        elif os.path.isfile(p):
            out.append(p)
    uniq = sorted(set(out))
    if not uniq:
        raise click.ClickException("No ABI files found")
    return uniq

# ------------------------------ DDL generators ------------------------------

def ddl_for_event(ev: EventModel, target: str, schema: Optional[str]="public") -> str:
    tbl = f"{ev.contract}_{ev.name}".lower()
    if target == "bigquery":
        # Dataset.table; caller can prepend dataset if needed
        tblname = f"{tbl}"
    elif target == "clickhouse":
        tblname = f"{tbl}"
    else:
        tblname = f"{schema}.{tbl}"

    # common columns
    cols: List[Tuple[str,str]] = [
        ("block_number", "BIGINT" if target != "clickhouse" else "UInt64"),
        ("block_time", "TIMESTAMP" if target != "clickhouse" else "DateTime"),
        ("tx_hash", "BYTEA" if target=="postgres" else ("BYTES" if target=="bigquery" else "FixedString(32)")),
        ("log_index", "INT" if target!="clickhouse" else "UInt32"),
        ("address", to_sql_type("address", target)),
        ("topic0", "BYTEA" if target=="postgres" else ("BYTES" if target=="bigquery" else "FixedString(32)")),
    ]
    # indexed and data params
    for p in ev.inputs:
        sqlt = to_sql_type(p.type, target)
        col = f"{'idx_' if p.indexed else 'data_'}{p.name or ('arg'+str(p.position))}".lower()
        cols.append((col, sqlt))

    # Build CREATE
    if target == "postgres":
        cols_sql = ",\n  ".join([f"{c} {t}" for c,t in cols])
        return f"""-- {ev.signature}
CREATE TABLE IF NOT EXISTS {tblname} (
  {cols_sql}
);"""
    if target == "bigquery":
        cols_sql = ",\n  ".join([f"`{c}` {t}" for c,t in cols])
        return f"""-- {ev.signature}
CREATE TABLE IF NOT EXISTS `{tblname}` (
  {cols_sql}
);"""
    if target == "clickhouse":
        cols_sql = ",\n  ".join([f"`{c}` {t}" for c,t in cols])
        return f"""-- {ev.signature}
CREATE TABLE IF NOT EXISTS {tblname} (
  {cols_sql}
)
ENGINE = MergeTree()
ORDER BY (block_number, log_index);"""
    return ""

# ------------------------------ JSON schema ------------------------------

def json_schema_for_event(ev: EventModel) -> Dict[str, Any]:
    props: Dict[str, Any] = {
        "block_number": {"type":"integer"},
        "block_time": {"type":"string", "format":"date-time"},
        "tx_hash": {"type":"string"},
        "log_index": {"type":"integer"},
        "address": {"type":"string"},
        "topic0": {"type":"string"}
    }
    for p in ev.inputs:
        key = f"{'idx_' if p.indexed else 'data_'}{p.name or ('arg'+str(p.position))}".lower()
        t = p.type
        if t.endswith("[]"):
            props[key] = {"type":"array", "items":{"type":"string"}}  # store as JSON/strings
        elif t in ("uint256","int256"):
            props[key] = {"type":"string", "pattern":"^-?\\d+$"}
        elif t in ("address","bytes32","bytes"):
            props[key] = {"type":"string"}
        elif t == "bool":
            props[key] = {"type":"boolean"}
        else:
            props[key] = {"type":"string"}
    return {
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "title": f"{ev.contract}.{ev.name}",
        "type":"object",
        "properties": props,
        "additionalProperties": False
    }

# ------------------------------ CLI ------------------------------

@click.group(context_settings=dict(help_option_names=["-h","--help"]))
def cli():
    """topicfoundry — Forge event schemas & filters from ABIs."""
    pass

@cli.command("build")
@click.argument("abi_paths", nargs=-1)
@click.option("--pretty", is_flag=True, help="Console summary.")
def build_cmd(abi_paths, pretty):
    """Print a concise summary for each event in given ABIs."""
    files = expand_paths(list(abi_paths))
    events: List[EventModel] = []
    for p in files:
        events.extend(extract_events(p))

    if pretty:
        for ev in events:
            click.echo(f"{ev.file}: {ev.name}  topic0={ev.topic0}")
            for p in ev.inputs:
                tag = "idx" if p.indexed else "dat"
                click.echo(f"   - [{tag}] {p.name}:{p.type}")
        click.echo(f"\nTotal events: {len(events)}")
    else:
        click.echo(json.dumps([asdict(e) for e in events], indent=2))

@cli.command("ddl")
@click.argument("abi_paths", nargs=-1)
@click.option("--target", type=click.Choice(["postgres","bigquery","clickhouse"]), required=True)
@click.option("--schema", default="public", show_default=True, help="Postgres schema (ignored for other targets).")
def ddl_cmd(abi_paths, target, schema):
    """Emit CREATE TABLE statements for all events."""
    files = expand_paths(list(abi_paths))
    out: List[str] = []
    for p in files:
        for ev in extract_events(p):
            out.append(ddl_for_event(ev, target, schema))
    click.echo("\n\n".join(out))

@cli.command("json")
@click.argument("abi_paths", nargs=-1)
@click.option("--out", type=click.Path(writable=True), default=None, help="Write a combined JSON with event schemas.")
def json_cmd(abi_paths, out):
    """Emit a JSON blob: manifest + JSON Schema per event."""
    files = expand_paths(list(abi_paths))
    items = []
    for p in files:
        for ev in extract_events(p):
            items.append({
                "file": ev.file,
                "contract": ev.contract,
                "event": ev.name,
                "signature": ev.signature,
                "topic0": ev.topic0,
                "schema": json_schema_for_event(ev)
            })
    doc = {"version":"topicfoundry.v1","events": items}
    s = json.dumps(doc, indent=2)
    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(s)
        click.echo(f"Wrote JSON schemas: {out}")
    else:
        click.echo(s)

@cli.command("dict")
@click.argument("abi_paths", nargs=-1)
@click.option("--out", type=click.Path(writable=True), default=None, help="Write CSV data dictionary.")
def dict_cmd(abi_paths, out):
    """Produce a CSV (stdout or file): event dictionary."""
    import csv
    files = expand_paths(list(abi_paths))
    rows = []
    for p in files:
        for ev in extract_events(p):
            for prm in ev.inputs:
                rows.append([
                    ev.contract, ev.name, ev.signature, ev.topic0,
                    prm.position, prm.name, prm.type, 1 if prm.indexed else 0
                ])
    if out:
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["contract","event","signature","topic0","position","param","type","indexed"])
            w.writerows(rows)
        click.echo(f"Wrote dictionary: {out}")
    else:
        w = csv.writer(sys.stdout)
        w.writerow(["contract","event","signature","topic0","position","param","type","indexed"])
        w.writerows(rows)

@cli.command("filters")
@click.argument("abi_paths", nargs=-1)
@click.option("--pretty", is_flag=True, help="Console view of per-event topic filters.")
def filters_cmd(abi_paths, pretty):
    """Print eth_getLogs topic stubs per event and per contract."""
    files = expand_paths(list(abi_paths))
    for p in files:
        evs = extract_events(p)
        if pretty:
            click.echo(f"== {os.path.basename(p)} ==")
        for ev in evs:
            # Build topics array with placeholders for indexed args
            idx_count = sum(1 for prm in ev.inputs if prm.indexed)
            topics = [ev.topic0] + ["<topic for indexed arg>"] * idx_count
            stub = {
                "address": "<contract_address_if_known>",
                "topics": topics
            }
            if pretty:
                click.echo(f"  {ev.name} — topics: {topics}")
            else:
                click.echo(json.dumps({"event": ev.name, "filter": stub}))
        if pretty:
            click.echo("")
    if not pretty:
        # nothing more to do; output already printed line by line
        pass

if __name__ == "__main__":
    cli()
