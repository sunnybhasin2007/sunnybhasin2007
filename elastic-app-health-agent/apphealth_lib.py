"""Shared helpers for components.py and setup_agent.py."""
import json
import os
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"

LIST_FIELDS = ["aliases", "depends_on", "log_indices", "metric_indices", "trace_indices"]
SIGNALS = ["log", "metric", "trace"]

# Columns of the components table, in CSV order.
COLUMNS = [
    "component", "display_name", "aliases", "description", "owner", "depends_on",
    "schema", "service_name",
    "log_indices", "log_filter", "metric_indices", "metric_filter",
    "trace_indices", "trace_filter",
    "timestamp_field", "host_field", "message_field", "service_field",
    "error_condition", "warning_condition", "trace_id_field", "notes",
]


def load_settings():
    with open(CONFIG_DIR / "agent.yaml") as f:
        settings = yaml.safe_load(f)
    with open(CONFIG_DIR / "query_templates.yaml") as f:
        settings["query_templates"] = yaml.safe_load(f)
    return settings


# --------------------------------------------------------------------------- index setup
def components_mapping():
    kw = {"type": "keyword"}
    props = {c: dict(kw) for c in COLUMNS}
    props["description"] = {"type": "text"}
    props["notes"] = {"type": "text"}
    props["updated_at"] = {"type": "date"}
    props["resolved"] = {"type": "object", "dynamic": True}
    props["queries"] = {"type": "object", "dynamic": True}
    return {
        "mappings": {
            "dynamic": "strict",
            "dynamic_templates": [
                {"queries": {"path_match": "queries.*", "match_mapping_type": "string",
                             "mapping": {"type": "keyword", "index": False}}},
                {"resolved": {"path_match": "resolved.*", "match_mapping_type": "string",
                              "mapping": {"type": "keyword"}}},
            ],
            "properties": props,
        },
    }


CHECKS_MAPPING = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "component": {"type": "keyword"},
            "check_id": {"type": "keyword"},
            "description": {"type": "text"},
            "query": {"type": "keyword", "index": False},
        },
    },
}

# Normalises a component row and renders the ready-to-run ES|QL queries into
# `queries.*`. Runs on every write, whichever way the row is written (CLI,
# Dev Tools, bulk), so the table and its queries never drift apart.
PAINLESS = r"""
List asList(def v) {
  List out = new ArrayList();
  if (v == null) { return out; }
  if (v instanceof List) {
    for (def x : v) { if (x != null) { String t = x.toString().trim(); if (t.length() > 0) { out.add(t); } } }
    return out;
  }
  for (String a : v.toString().splitOnToken(';')) {
    for (String b : a.splitOnToken(',')) { String t = b.trim(); if (t.length() > 0) { out.add(t); } }
  }
  return out;
}
String str(def v) { return v == null ? '' : v.toString().trim(); }

String comp = str(ctx.component).toLowerCase();
if (comp.length() == 0) { throw new IllegalArgumentException('component is required'); }
ctx.component = comp;
for (String f : params.list_fields) { ctx[f] = asList(ctx[f]); }
List aliases = new ArrayList();
for (def a : ctx.aliases) { aliases.add(a.toLowerCase()); }
ctx.aliases = aliases;
if (str(ctx.display_name).length() == 0) { ctx.display_name = comp; }

String schema = str(ctx.schema).toLowerCase();
if (schema.length() == 0) { schema = params.default_schema; }
Map preset = params.presets.get(schema);
if (preset == null) { throw new IllegalArgumentException('unknown schema [' + schema + '], expected one of ' + params.presets.keySet()); }
ctx.schema = schema;

boolean hasData = false;
for (String sig : params.signals) { if (!ctx[sig + '_indices'].isEmpty()) { hasData = true; } }
if (!hasData) { throw new IllegalArgumentException('component [' + comp + '] needs at least one of log_indices, metric_indices, trace_indices'); }

Map r = new HashMap();
for (def k : preset.keySet()) { String v = str(ctx[k]); r[k] = v.length() > 0 ? v : preset[k]; }
String svc = str(ctx.service_name);
for (String sig : params.signals) {
  String f = str(ctx[sig + '_filter']);
  if (f.length() == 0 && svc.length() > 0) { f = r.service_field + ' == "' + svc + '"'; }
  if (f.length() == 0) { f = 'true'; }
  r[sig + '_filter'] = f;
}

Map queries = new HashMap();
for (def t : params.templates) {
  if (t.schema != null && t.schema != schema) { continue; }
  List idx = ctx[t.signal + '_indices'];
  if (idx.isEmpty()) { continue; }
  String where = r.timestamp_field + ' >= NOW() - {MINUTES} minutes AND (' + r[t.signal + '_filter'] + ')';
  String q = t.query.replace('{INDICES}', String.join(', ', idx)).replace('{WHERE}', where);
  q = q.replace('{TS}', r.timestamp_field).replace('{HOST}', r.host_field).replace('{MSG}', r.message_field);
  q = q.replace('{ERROR}', r.error_condition).replace('{WARNING}', r.warning_condition).replace('{TRACE_FIELD}', r.trace_id_field);
  queries[t.name] = q.trim();
}
ctx.resolved = r;
ctx.queries = queries;
"""


def pipeline_body(settings):
    qt = settings["query_templates"]
    templates = []
    for name, t in qt["templates"].items():
        templates.append({"name": name, "signal": t["signal"], "schema": t.get("schema"),
                          "query": t["query"]})
    return {
        "description": "app-health: normalise component rows and render ES|QL queries",
        "processors": [
            {"script": {"lang": "painless", "source": PAINLESS, "params": {
                "list_fields": LIST_FIELDS,
                "signals": SIGNALS,
                "default_schema": settings.get("default_schema", "otel"),
                "presets": qt["presets"],
                "templates": templates,
            }}},
            {"set": {"field": "updated_at", "value": "{{{_ingest.timestamp}}}"}},
        ],
    }


def template_purposes(settings):
    return {name: t["purpose"] for name, t in settings["query_templates"]["templates"].items()}


# --------------------------------------------------------------------------- http
def env(name):
    val = os.environ.get(name)
    if not val:
        sys.exit(f"environment variable {name} is required")
    return val


def error_reason(resp):
    """Innermost Elasticsearch/Kibana error reason, or the raw body."""
    try:
        err = resp.json()
    except ValueError:
        return resp.text[:3000]
    if isinstance(err.get("error"), dict):
        err = err["error"]
        while isinstance(err.get("caused_by"), dict):
            err = err["caused_by"]
        return err.get("reason") or json.dumps(err)[:3000]
    return err.get("message") or json.dumps(err)[:3000]


class Client:
    """Minimal Elasticsearch + Kibana REST client (API key or basic auth)."""

    def __init__(self, settings=None, insecure=False, need_kibana=False):
        import requests
        self.es = env("ES_URL").rstrip("/")
        self.kibana = (env("KIBANA_URL") if need_kibana else os.environ.get("KIBANA_URL", "")).rstrip("/")
        space = (settings or {}).get("kibana_space", "default")
        self.kbn_base = self.kibana + ("" if space == "default" else f"/s/{space}")
        self.session = requests.Session()
        self.session.headers.update({"kbn-xsrf": "true", "Content-Type": "application/json"})
        if os.environ.get("ELASTIC_API_KEY"):
            self.session.headers["Authorization"] = "ApiKey " + os.environ["ELASTIC_API_KEY"]
        else:
            self.session.auth = (env("ELASTIC_USERNAME"), env("ELASTIC_PASSWORD"))
        if insecure:
            self.session.verify = False
            requests.packages.urllib3.disable_warnings()
        elif os.environ.get("ELASTIC_CA_CERT"):
            self.session.verify = os.environ["ELASTIC_CA_CERT"]

    def call(self, method, url, body=None, ok=(200, 201), allow=(), raw=None, headers=None):
        data = raw if raw is not None else (None if body is None else json.dumps(body))
        resp = self.session.request(method, url, data=data, headers=headers, timeout=300)
        if resp.status_code in allow:
            return resp
        if resp.status_code not in ok:
            sys.exit(f"{method} {url} -> {resp.status_code}\n{error_reason(resp)}")
        return resp

    def esr(self, method, path, body=None, **kw):
        return self.call(method, self.es + path, body, **kw)

    def kbn(self, method, path, body=None, **kw):
        return self.call(method, self.kbn_base + path, body, **kw)

    def esql(self, query):
        """Run ES|QL; returns (columns, rows) or raises RuntimeError with the ES error."""
        resp = self.esr("POST", "/_query?format=json", {"query": query}, ok=(200,),
                        allow=(400, 404, 500))
        data = resp.json()
        if resp.status_code != 200:
            err = data.get("error", {})
            reason = err.get("reason") if isinstance(err, dict) else err
            raise RuntimeError(reason or resp.text[:500])
        return [c["name"] for c in data["columns"]], data["values"]
