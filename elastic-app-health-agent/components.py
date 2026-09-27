#!/usr/bin/env python3
"""Manage the component table the App Health Agent reads.

One row per component (kafka, zookeeper, payments-api, ...) listing the indices
that hold its logs, metrics and traces. The agent looks the component up in this
table at question time, so adding a row makes it available immediately; there is
no need to redeploy the agent.

  init                      create/update the ingest pipeline and the two indices
  add NAME [options]        add or update one component (only given fields change)
  remove NAME               delete a component and its checks
  list                      list all components
  show NAME                 show a component, its rendered queries and checks
  import FILE               bulk add/replace components from .csv / .yaml / .json
  export FILE.csv           write the table to CSV (edit in Excel, import back)
  rerender                  re-render all stored queries (after editing query_templates.yaml)
  add-check NAME ID ...     add a custom ES|QL check to a component
  remove-check NAME ID      remove a custom check
  import-checks FILE.yaml   bulk add checks
  validate [NAME ...]       run every query of every (or the given) component and report errors
  role                      create a read-only role limited to the registered indices

Connection: ES_URL, ELASTIC_API_KEY (or ELASTIC_USERNAME/ELASTIC_PASSWORD),
ELASTIC_CA_CERT; KIBANA_URL only for `role`.
"""
import argparse
import csv
import json
import re
import sys
from pathlib import Path

import yaml

from apphealth_lib import (CHECKS_MAPPING, COLUMNS, LIST_FIELDS, Client, components_mapping,
                           load_settings, pipeline_body)

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


class Table:
    def __init__(self, client, settings):
        self.c = client
        self.s = settings
        self.index = settings["components_index"]
        self.checks = settings["checks_index"]
        self.pipeline = self.index + "-render"

    # ------------------------------------------------------------------ setup
    def init(self):
        self.c.esr("PUT", f"/_ingest/pipeline/{self.pipeline}", pipeline_body(self.s))
        print(f"pipeline {self.pipeline}: updated")
        if self.c.esr("HEAD", f"/{self.index}", ok=(200,), allow=(404,)).status_code == 404:
            body = components_mapping()
            body["settings"] = {"index": {"default_pipeline": self.pipeline,
                                          "number_of_replicas": 0, "auto_expand_replicas": "0-1"}}
            self.c.esr("PUT", f"/{self.index}", body)
            print(f"index {self.index}: created")
        else:
            self.c.esr("PUT", f"/{self.index}/_mapping", components_mapping()["mappings"])
            print(f"index {self.index}: exists (mapping updated)")
        if self.c.esr("HEAD", f"/{self.checks}", ok=(200,), allow=(404,)).status_code == 404:
            body = dict(CHECKS_MAPPING)
            body["settings"] = {"index": {"number_of_replicas": 0, "auto_expand_replicas": "0-1"}}
            self.c.esr("PUT", f"/{self.checks}", body)
            print(f"index {self.checks}: created")
        else:
            print(f"index {self.checks}: exists")

    # ------------------------------------------------------------------ rows
    def get(self, name):
        resp = self.c.esr("GET", f"/{self.index}/_doc/{name}", ok=(200,), allow=(404,))
        return resp.json()["_source"] if resp.status_code == 200 else None

    def all_rows(self):
        resp = self.c.esr("POST", f"/{self.index}/_search",
                          {"size": 10000, "sort": [{"component": "asc"}],
                           "_source": {"includes": COLUMNS}})
        return [h["_source"] for h in resp.json()["hits"]["hits"]]

    def put(self, row):
        name = check_name(row.get("component"))
        row = {k: v for k, v in row.items() if k in COLUMNS and v is not None}
        row["component"] = name
        self.c.esr("PUT", f"/{self.index}/_doc/{name}?refresh=true", row)

    def bulk_put(self, rows):
        lines = []
        for row in rows:
            name = check_name(row.get("component"))
            row = {k: v for k, v in row.items() if k in COLUMNS and v not in (None, "")}
            row["component"] = name
            lines += [json.dumps({"index": {"_index": self.index, "_id": name}}), json.dumps(row)]
        if not lines:
            return 0
        resp = self.c.esr("POST", "/_bulk?refresh=true", raw="\n".join(lines) + "\n",
                          headers={"Content-Type": "application/x-ndjson"})
        errors = [i["index"] for i in resp.json()["items"] if i["index"].get("error")]
        for e in errors:
            print(f"  ERROR {e['_id']}: {e['error'].get('caused_by', e['error']).get('reason')}")
        return len(rows) - len(errors)

    def remove(self, name):
        resp = self.c.esr("DELETE", f"/{self.index}/_doc/{name}?refresh=true", ok=(200,), allow=(404,))
        self.c.esr("POST", f"/{self.checks}/_delete_by_query?refresh=true",
                   {"query": {"term": {"component": name}}})
        print(f"{name}: {'removed' if resp.status_code == 200 else 'not found'}")

    # ------------------------------------------------------------------ checks
    def put_check(self, component, check_id, description, query):
        if self.get(component) is None:
            sys.exit(f"unknown component {component!r}; add it first")
        doc = {"component": component, "check_id": check_id,
               "description": description, "query": query.strip()}
        self.c.esr("PUT", f"/{self.checks}/_doc/{component}__{check_id}?refresh=true", doc)

    def checks_for(self, component):
        resp = self.c.esr("POST", f"/{self.checks}/_search",
                          {"size": 1000, "query": {"term": {"component": component}}})
        return [h["_source"] for h in resp.json()["hits"]["hits"]]


def check_name(name):
    name = str(name or "").strip().lower()
    if not NAME_RE.match(name):
        sys.exit(f"invalid component name {name!r}: use lowercase letters, digits, . _ -")
    return name


def fill(query, minutes):
    return (query.replace("{MINUTES}", str(minutes)).replace("{TEXT}", "error")
            .replace("{TRACE_ID}", "0"))


def read_rows(path):
    path = Path(path)
    if path.suffix.lower() == ".csv":
        with open(path, newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        unknown = set(rows[0].keys()) - set(COLUMNS) if rows else set()
        if unknown:
            sys.exit(f"unknown CSV columns: {sorted(unknown)}; allowed: {COLUMNS}")
        return rows
    with open(path) as f:
        data = yaml.safe_load(f)  # YAML is a superset of JSON
    return data.get("components", data) if isinstance(data, dict) else data


def print_table(rows, cols):
    widths = {c: min(max([len(c)] + [len(fmt(r.get(c))) for r in rows]), 60) for c in cols}
    print("  ".join(c.ljust(widths[c]) for c in cols))
    for r in rows:
        print("  ".join(fmt(r.get(c))[:60].ljust(widths[c]) for c in cols))


def fmt(v):
    if isinstance(v, list):
        return ", ".join(map(str, v))
    return "" if v is None else str(v)


# ---------------------------------------------------------------------- commands
def cmd_add(t, a):
    name = check_name(a.name)
    existing = {} if a.replace else (t.get(name) or {})
    row = {k: v for k, v in existing.items() if k in COLUMNS}
    for col in COLUMNS[1:]:
        val = getattr(a, col, None)
        if val is not None:
            row[col] = val
    row["component"] = name
    t.put(row)
    stored = t.get(name)
    print(f"{name}: {'updated' if existing else 'added'}; queries: {', '.join(sorted(stored.get('queries', {})))}")


def cmd_show(t, a):
    row = t.get(check_name(a.name))
    if row is None:
        sys.exit(f"{a.name}: not found")
    queries = row.pop("queries", {})
    resolved = row.pop("resolved", {})
    row = {k: row[k] for k in COLUMNS + ["updated_at"] if row.get(k) not in (None, "", [])}
    print(yaml.safe_dump(row, sort_keys=False, allow_unicode=True))
    print("resolved:\n" + yaml.safe_dump(resolved, sort_keys=True))
    for name in sorted(queries):
        print(f"--- queries.{name}\n{queries[name]}\n")
    for chk in t.checks_for(row["component"]):
        print(f"--- check {chk['check_id']}: {chk['description']}\n{chk['query']}\n")


def cmd_validate(t, a):
    names = [check_name(n) for n in a.names] or [r["component"] for r in t.all_rows()]
    failures = 0
    for name in names:
        row = t.get(name)
        if row is None:
            print(f"{name}: not found")
            failures += 1
            continue
        items = [(f"queries.{k}", v) for k, v in sorted(row.get("queries", {}).items())]
        items += [(f"check.{c['check_id']}", c["query"]) for c in t.checks_for(name)]
        print(f"{name}")
        for label, query in items:
            try:
                _, values = t.c.esql(fill(query, a.minutes))
                print(f"  ok    {label:32} {len(values)} rows")
            except RuntimeError as exc:
                failures += 1
                print(f"  FAIL  {label:32} {str(exc)[:300]}")
    print(f"\n{failures} failing queries" if failures else "\nall queries ok")
    return failures


def cmd_role(t, a, settings):
    rows = t.all_rows()
    indices = sorted({i for r in rows for f in ("log_indices", "metric_indices", "trace_indices")
                      for i in (r.get(f) or [])})
    names = indices + [t.index, t.checks]
    body = {
        "elasticsearch": {"cluster": [],
                          "indices": [{"names": names, "privileges": ["read", "view_index_metadata"]}]},
        "kibana": [{"spaces": [settings.get("kibana_space", "default")], "base": [],
                    "feature": settings.get("role_kibana_features", {})}],
    }
    if a.dry_run or not t.c.kibana:
        print(json.dumps(body, indent=2))
        if not a.dry_run:
            print("KIBANA_URL not set: printed only. Use PUT kbn:/api/security/role/"
                  f"{a.name} in Dev Tools.", file=sys.stderr)
        return
    t.c.call("PUT", f"{t.c.kibana}/api/security/role/{a.name}", body, ok=(200, 204))
    print(f"role {a.name}: read on {len(indices)} registered index patterns + the tables. "
          f"Assign it to the users who should use the agent.")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--insecure", action="store_true", help="skip TLS verification")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")

    add = sub.add_parser("add", help="add or update a component")
    add.add_argument("name")
    add.add_argument("--replace", action="store_true", help="replace the row instead of merging")
    helps = {
        "display_name": "human readable name",
        "aliases": "other names users may type, comma separated",
        "depends_on": "components this one depends on (used for root cause), comma separated",
        "schema": "otel (default) or ecs",
        "service_name": "OTel service.name; builds the default filters",
        "log_indices": "comma separated index patterns", "metric_indices": "comma separated",
        "trace_indices": "comma separated",
        "log_filter": 'ES|QL condition, e.g. resource.attributes.service.name == "kafka"',
    }
    for col in COLUMNS[1:]:
        add.add_argument("--" + col.replace("_", "-"), dest=col, help=helps.get(col))

    for name in ("remove", "show"):
        sub.add_parser(name).add_argument("name")
    sub.add_parser("list")
    sub.add_parser("import").add_argument("file")
    sub.add_parser("export").add_argument("file")
    sub.add_parser("rerender")

    ac = sub.add_parser("add-check")
    ac.add_argument("name")
    ac.add_argument("check_id")
    ac.add_argument("--description", required=True)
    grp = ac.add_mutually_exclusive_group(required=True)
    grp.add_argument("--query")
    grp.add_argument("--query-file")
    rc = sub.add_parser("remove-check")
    rc.add_argument("name")
    rc.add_argument("check_id")
    sub.add_parser("import-checks").add_argument("file")

    v = sub.add_parser("validate")
    v.add_argument("names", nargs="*")
    v.add_argument("--minutes", type=int, default=60)

    r = sub.add_parser("role")
    r.add_argument("--name", default="app_health_agent_user")
    r.add_argument("--dry-run", action="store_true")

    a = p.parse_args()
    settings = load_settings()
    t = Table(Client(settings, insecure=a.insecure), settings)

    if a.cmd == "init":
        t.init()
    elif a.cmd == "add":
        cmd_add(t, a)
    elif a.cmd == "remove":
        t.remove(check_name(a.name))
    elif a.cmd == "list":
        rows = t.all_rows()
        for r in rows:
            r["signals"] = " ".join(s for s in ("log", "metric", "trace") if r.get(f"{s}_indices"))
        print_table(rows, ["component", "display_name", "aliases", "service_name", "signals",
                           "depends_on", "owner"])
        print(f"\n{len(rows)} components")
    elif a.cmd == "show":
        cmd_show(t, a)
    elif a.cmd == "import":
        rows = read_rows(a.file)
        print(f"imported {t.bulk_put(rows)}/{len(rows)} components")
    elif a.cmd == "export":
        rows = t.all_rows()
        with open(a.file, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            for r in rows:
                w.writerow({c: "; ".join(r[c]) if c in LIST_FIELDS and isinstance(r.get(c), list)
                            else r.get(c, "") for c in COLUMNS})
        print(f"exported {len(rows)} components to {a.file}")
    elif a.cmd == "rerender":
        rows = t.all_rows()
        print(f"re-rendered {t.bulk_put(rows)}/{len(rows)} components")
    elif a.cmd == "add-check":
        query = a.query or Path(a.query_file).read_text()
        t.put_check(check_name(a.name), a.check_id, a.description, query)
        print(f"check {a.check_id} saved for {a.name}")
    elif a.cmd == "remove-check":
        t.c.esr("DELETE", f"/{t.checks}/_doc/{check_name(a.name)}__{a.check_id}?refresh=true",
                ok=(200,), allow=(404,))
        print("removed")
    elif a.cmd == "import-checks":
        with open(a.file) as f:
            data = yaml.safe_load(f)
        n = 0
        for comp, checks in data.items():
            for chk in checks:
                t.put_check(check_name(comp), chk["id"], chk["description"], chk["query"])
                n += 1
        print(f"imported {n} checks")
    elif a.cmd == "validate":
        sys.exit(1 if cmd_validate(t, a) else 0)
    elif a.cmd == "role":
        cmd_role(t, a, settings)


if __name__ == "__main__":
    main()
