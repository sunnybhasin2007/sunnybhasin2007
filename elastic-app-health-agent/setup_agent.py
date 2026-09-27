#!/usr/bin/env python3
"""Generate and deploy an application-scoped health agent for Kibana Agent Builder.

Reads config/apps.yaml (app -> indices catalog) and config/agent.yaml, then:
  * writes an app registry index in Elasticsearch,
  * creates one set of index-scoped tools per application,
  * creates/updates an agent whose instructions force it to resolve the
    application first and only query that application's indices.

Commands:
  render   write everything to ./out (JSON + a Kibana Dev Tools script); no network
  apply    create/update registry, tools and agent (removes stale generated tools)
  test     execute every generated tool once and report errors
  ask      send a question to the agent, e.g.  ask "health of kafka"
  destroy  delete the agent, generated tools and the registry index

Connection (environment variables):
  KIBANA_URL        e.g. https://kibana.example.local:5601
  ES_URL            e.g. https://es.example.local:9200
  ELASTIC_API_KEY   base64 "id:key" API key            (or)
  ELASTIC_USERNAME / ELASTIC_PASSWORD
  ELASTIC_CA_CERT   path to CA bundle for on-prem TLS (optional)
"""
import argparse
import copy
import json
import os
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
GENERATED_TAG = "app-health-generated"
TIME_FILTER = (
    '{ts} >= NOW() - {max_lookback} '
    'AND DATE_DIFF("minute", {ts}, NOW()) <= ?lookback_minutes'
)
LOOKBACK_PARAM = {
    "type": "integer",
    "description": "How many minutes back to look (e.g. 15, 60, 240, 1440). "
                   "Use 60 unless the user asks for another window.",
}


# --------------------------------------------------------------------------- config
def load_config(apps_path, agent_path):
    with open(agent_path) as f:
        agent_cfg = yaml.safe_load(f)
    with open(apps_path) as f:
        apps_cfg = yaml.safe_load(f)
    defaults = apps_cfg.get("defaults", {})
    apps = []
    seen = set()
    for raw in apps_cfg.get("applications", []):
        app = {**defaults, **raw}
        app.setdefault("aliases", [])
        app.setdefault("depends_on", [])
        app.setdefault("log_indices", [])
        app.setdefault("metric_indices", [])
        app.setdefault("checks", [])
        app["slug"] = slugify(app["id"])
        if app["slug"] in seen:
            sys.exit(f"duplicate application id: {app['id']}")
        if not app["log_indices"] and not app["metric_indices"]:
            sys.exit(f"application {app['id']} has no log_indices or metric_indices")
        seen.add(app["slug"])
        apps.append(app)
    ids = {a["id"] for a in apps}
    for app in apps:
        for dep in app["depends_on"]:
            if dep not in ids:
                print(f"warning: {app['id']} depends_on unknown app '{dep}'", file=sys.stderr)
    return agent_cfg, apps


def slugify(value):
    slug = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
    if not slug or not slug[0].isalpha():
        sys.exit(f"application id must start with a letter: {value!r}")
    return slug


# --------------------------------------------------------------------------- tools
def esql_tool(tool_id, description, query, tags):
    query = query.strip()
    params = {}
    if "?lookback_minutes" in query:
        params["lookback_minutes"] = LOOKBACK_PARAM
    return {
        "id": tool_id,
        "type": "esql",
        "description": description.strip(),
        "tags": tags,
        "configuration": {"query": query, "params": params},
    }


def time_filter(app):
    return TIME_FILTER.format(ts=app["timestamp_field"], max_lookback=app["max_lookback"])


def build_app_tools(prefix, app):
    ts, msg, host = app["timestamp_field"], app["message_field"], app["host_field"]
    err, warn = app["error_condition"], app["warning_condition"]
    tf = time_filter(app)
    base = f"{prefix}.{app['slug']}"
    tags = [GENERATED_TAG, f"app:{app['slug']}"]
    name = app["display_name"]
    tools = []

    if app["log_indices"]:
        logs = ", ".join(app["log_indices"])
        tools.append(esql_tool(
            f"{base}.health_summary",
            f"HEALTH CHECK for {name} (app id '{app['id']}'). Per host: total log "
            f"lines, errors, warnings, error rate and minutes since the last log line, "
            f"over the lookback window. Reads only: {logs}. A host with a large "
            f"minutes_since_last_event is silent (possibly down).",
            f"""
FROM {logs}
| WHERE {tf}
| EVAL is_error = CASE(({err}), 1, 0), is_warning = CASE(({warn}), 1, 0)
| STATS total_events = COUNT(*), errors = SUM(is_error), warnings = SUM(is_warning),
        last_event = MAX({ts})
    BY {host}
| EVAL error_rate_pct = ROUND(errors * 100.0 / total_events, 2),
       minutes_since_last_event = DATE_DIFF("minute", last_event, NOW())
| SORT errors DESC
| LIMIT 100
""", tags))

        if app["error_grouping"] == "categorize":
            group = f"error_pattern = CATEGORIZE({msg})"
        else:
            group = "error_pattern"
        pre_group = "" if app["error_grouping"] == "categorize" else \
            f"| EVAL error_pattern = SUBSTRING(TO_STRING({msg}), 1, 200)\n"
        tools.append(esql_tool(
            f"{base}.find_errors",
            f"FIND ISSUES in {name} (app id '{app['id']}'). Top error messages grouped "
            f"by pattern with count, first/last seen and number of affected hosts. "
            f"Use for 'any issues/errors/exceptions' and as the first step of root cause "
            f"analysis. Reads only: {logs}.",
            f"""
FROM {logs}
| WHERE {tf}
| WHERE ({err})
{pre_group}| STATS occurrences = COUNT(*), first_seen = MIN({ts}), last_seen = MAX({ts}),
        affected_hosts = COUNT_DISTINCT({host})
    BY {group}
| SORT occurrences DESC
| LIMIT 25
""", tags))

        tools.append(esql_tool(
            f"{base}.error_timeline",
            f"TIMELINE for {name} (app id '{app['id']}'): events, errors and warnings per "
            f"{app['timeline_bucket']} bucket. Use to find WHEN a problem started "
            f"(root cause) and whether it is ongoing. Keep lookback <= 1440. "
            f"Reads only: {logs}.",
            f"""
FROM {logs}
| WHERE {tf}
| EVAL is_error = CASE(({err}), 1, 0), is_warning = CASE(({warn}), 1, 0)
| STATS events = COUNT(*), errors = SUM(is_error), warnings = SUM(is_warning)
    BY bucket = BUCKET({ts}, {app['timeline_bucket']})
| SORT bucket ASC
| LIMIT 500
""", tags))

        tools.append({
            "id": f"{base}.search_logs",
            "type": "index_search",
            "description": (
                f"Free-text / natural-language search over {name} logs only "
                f"({logs}). Use to look up specific exceptions, ids, topics, "
                f"hosts or messages found during analysis."
            ),
            "tags": tags,
            "configuration": {"pattern": ",".join(app["log_indices"])},
        })

    if app["metric_indices"]:
        metrics = ", ".join(app["metric_indices"])
        tools.append(esql_tool(
            f"{base}.metrics_freshness",
            f"Checks that {name} (app id '{app['id']}') metrics are still arriving: "
            f"documents and minutes since last document per index over the lookback "
            f"window. Reads only: {metrics}.",
            f"""
FROM {metrics} METADATA _index
| WHERE {tf}
| STATS docs = COUNT(*), last_doc = MAX({ts}) BY _index
| EVAL minutes_since_last_doc = DATE_DIFF("minute", last_doc, NOW())
| SORT minutes_since_last_doc DESC
| LIMIT 50
""", tags))

    for check in app["checks"]:
        query = check["query"]
        for key, val in {
            "{logs}": ", ".join(app["log_indices"]),
            "{metrics}": ", ".join(app["metric_indices"]),
            "{time_filter}": tf,
            "{ts}": ts,
        }.items():
            query = query.replace(key, val)
        tools.append(esql_tool(
            f"{base}.{slugify(check['id'])}",
            f"{name} (app id '{app['id']}') check: {check['description']}",
            query, tags))
    return tools


def build_registry_tools(prefix, registry_index):
    tags = [GENERATED_TAG, "app-registry"]
    return [
        esql_tool(
            f"{prefix}.list_applications",
            "ALWAYS CALL FIRST. Lists every monitored application with its app_id, "
            "aliases, owner, dependencies, the indices that belong to it and the ids "
            "of the tools to use for it. Use it to map the user's wording "
            "(e.g. 'kafka brokers', 'zk') to an app_id.",
            f"""
FROM {registry_index}
| KEEP app_id, display_name, aliases, description, owner, depends_on,
       log_indices, metric_indices, tool_ids
| SORT app_id
| LIMIT 500
""", tags),
    ]


def registry_docs(apps, tools_by_app):
    return [{
        "app_id": app["id"],
        "display_name": app["display_name"],
        "aliases": app["aliases"],
        "description": app.get("description", ""),
        "owner": app.get("owner", ""),
        "depends_on": app["depends_on"],
        "log_indices": app["log_indices"],
        "metric_indices": app["metric_indices"],
        "tool_ids": [t["id"] for t in tools_by_app[app["id"]]],
    } for app in apps]


REGISTRY_MAPPING = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "app_id": {"type": "keyword"},
            "display_name": {"type": "keyword"},
            "aliases": {"type": "keyword"},
            "description": {"type": "text"},
            "owner": {"type": "keyword"},
            "depends_on": {"type": "keyword"},
            "log_indices": {"type": "keyword"},
            "metric_indices": {"type": "keyword"},
            "tool_ids": {"type": "keyword"},
        },
    },
}


# --------------------------------------------------------------------------- agent
INSTRUCTIONS = """\
You are the Application Health Agent. You answer questions about the health,
issues and root cause of problems in specific applications (for example Kafka)
using ONLY the Elasticsearch indices registered for that application.

## Hard rules
1. Never search the whole cluster. Never query an index that is not listed for the
   application in `{prefix}.list_applications`.
2. Always start by calling `{prefix}.list_applications` and resolve the user's
   wording to exactly one `app_id` (match on app_id, display_name or aliases).
   If nothing matches, say so and list the available applications. If several
   match, ask which one they mean.
3. Tool ids have the form `{prefix}.<app>.<check>`. Only use the tools whose ids appear in that application's `tool_ids`
   (and those of its `depends_on` apps during root-cause analysis).
4. If you use `platform.core.execute_esql` or `platform.core.get_index_mapping`,
   the FROM clause / index MUST be one of the app's `log_indices` or
   `metric_indices`, and you must always include a time filter and a LIMIT.
5. Default lookback is {default_lookback} minutes unless the user asks for another
   window ("last 4 hours" -> 240, "today"/"last day" -> 1440).
6. Base every statement on tool results. Quote numbers, hosts and timestamps.
   If a tool errors (e.g. index or field not found) or returns no rows, say
   exactly that - "no data" is itself a finding (the app may be down or not shipping logs).

## Workflows
**Health check** ("how is kafka", "health of X", "is X ok"):
 - Run `{prefix}.<app>.health_summary`, `{prefix}.<app>.metrics_freshness` (if present) and every
   app-specific check tool (e.g. consumer_lag). Run `{prefix}.<app>.find_errors` if errors > 0.
 - Answer with: overall status (HEALTHY / DEGRADED / DOWN) with a one-line reason,
   a short table of key numbers, top issues, and recommended next steps.
 - Guide: DOWN = no recent data at all or all hosts silent > 15 min;
   DEGRADED = error rate > 5%, a silent host, rising errors, or a failing check;
   otherwise HEALTHY.

**Find issues** ("any issues/errors in X"):
 - Run `{prefix}.<app>.find_errors`, then `{prefix}.<app>.error_timeline` for the top patterns' window.
 - List issues ranked by impact (occurrences, affected hosts, still ongoing?).

**Root cause analysis** ("why is X failing", "root cause"):
 1. `{prefix}.<app>.error_timeline` to find when errors/volume changed (the onset time).
 2. `{prefix}.<app>.find_errors` around the onset; identify the earliest new error pattern.
 3. `{prefix}.<app>.search_logs` for that pattern / exception to get concrete examples.
 4. Check the app-specific checks and metrics for the same window.
 5. For each app in `depends_on`, run its health_summary and find_errors for the
    same window - an upstream failure that started first is a likely cause.
 6. Report: timeline of events, most likely root cause with evidence, confidence
    (high/medium/low), what else could explain it, and concrete next steps.
    Clearly separate evidence from hypothesis.

## Known applications
{catalog}
"""


def build_agent(agent_cfg, apps, tool_ids):
    prefix = agent_cfg["tool_prefix"]
    catalog = "\n".join(
        f"- `{a['id']}` ({a['display_name']}); aliases: {', '.join(a['aliases']) or '-'}"
        for a in apps
    )
    a = agent_cfg["agent"]
    instructions = INSTRUCTIONS.format(
        prefix=prefix,
        default_lookback=agent_cfg.get("default_lookback_minutes", 60),
        catalog=catalog,
    )
    return {
        "id": a["id"],
        "name": a["name"],
        "description": a["description"].strip(),
        "labels": a.get("labels", []),
        "avatar_color": a.get("avatar_color"),
        "avatar_symbol": a.get("avatar_symbol"),
        "configuration": {
            "instructions": instructions,
            "tools": [{"tool_ids": tool_ids + agent_cfg.get("extra_platform_tools", [])}],
        },
    }


def build_all(agent_cfg, apps):
    prefix = agent_cfg["tool_prefix"]
    tools_by_app = {app["id"]: build_app_tools(prefix, app) for app in apps}
    registry_tools = build_registry_tools(prefix, agent_cfg["registry_index"])
    tools = registry_tools + [t for app in apps for t in tools_by_app[app["id"]]]
    ids = [t["id"] for t in tools]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        sys.exit(f"duplicate tool ids: {sorted(dupes)}")
    return {
        "tools": tools,
        "agent": build_agent(agent_cfg, apps, ids),
        "registry": registry_docs(apps, tools_by_app),
        "role": build_role(agent_cfg, apps),
    }


def build_role(agent_cfg, apps):
    indices = sorted({i for a in apps for i in a["log_indices"] + a["metric_indices"]})
    return {
        "cluster": ["monitor"],
        "indices": [
            {"names": indices + [agent_cfg["registry_index"]],
             "privileges": ["read", "view_index_metadata"]},
        ],
    }


# --------------------------------------------------------------------------- render
def update_body(obj, drop):
    body = copy.deepcopy(obj)
    for key in drop:
        body.pop(key, None)
    return body


def render(bundle, agent_cfg, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "tools.json").write_text(json.dumps(bundle["tools"], indent=2))
    (out_dir / "agent.json").write_text(json.dumps(bundle["agent"], indent=2))
    (out_dir / "registry.json").write_text(json.dumps(bundle["registry"], indent=2))
    (out_dir / "agent_instructions.md").write_text(
        bundle["agent"]["configuration"]["instructions"])

    space = agent_cfg.get("kibana_space", "default")
    kbn = "kbn:" if space == "default" else f"kbn:/s/{space}"
    reg = agent_cfg["registry_index"]
    lines = [
        "# Kibana Dev Tools script generated by setup_agent.py",
        "# Paste into Kibana > Dev Tools > Console and run top to bottom.",
        "# Re-running: the POST for an existing tool/agent fails with 409;",
        "# run the matching PUT (commented below each POST) instead.",
        "",
        "# ---- 1. App registry index",
        f"DELETE {reg}",
        "",
        f"PUT {reg}",
        json.dumps(REGISTRY_MAPPING, indent=2),
        "",
        f"POST {reg}/_bulk?refresh=true",
    ]
    for doc in bundle["registry"]:
        lines.append(json.dumps({"index": {"_id": doc["app_id"]}}))
        lines.append(json.dumps(doc))
    lines += ["", "# ---- 2. Tools"]
    for tool in bundle["tools"]:
        lines += [f"POST {kbn}/api/agent_builder/tools", json.dumps(tool, indent=2)]
        lines += [f"# PUT {kbn}/api/agent_builder/tools/{tool['id']}  (body without id/type)", ""]
    agent = bundle["agent"]
    lines += ["# ---- 3. Agent",
              f"POST {kbn}/api/agent_builder/agents", json.dumps(agent, indent=2), "",
              f"# To update later:",
              f"# PUT {kbn}/api/agent_builder/agents/{agent['id']}  (body without id)", "",
              "# ---- 4. (Optional) role for chat users: read-only on registered indices only",
              "# Add Kibana privileges (Agent Builder + Actions/Connectors) in",
              "# Stack Management > Roles after creating it.",
              "PUT _security/role/app_health_agent_user", json.dumps(bundle["role"], indent=2), ""]
    (out_dir / "devtools_console.txt").write_text("\n".join(lines))
    print(f"wrote {len(bundle['tools'])} tools, agent and registry to {out_dir}/")


# --------------------------------------------------------------------------- http
class Client:
    def __init__(self, agent_cfg, insecure=False):
        import requests
        self.requests = requests
        self.kibana = env("KIBANA_URL").rstrip("/")
        self.es = os.environ.get("ES_URL", "").rstrip("/")
        space = agent_cfg.get("kibana_space", "default")
        self.kbn_base = self.kibana + ("" if space == "default" else f"/s/{space}")
        self.session = requests.Session()
        self.session.headers.update({"kbn-xsrf": "true", "Content-Type": "application/json",
                                     "elastic-api-version": "2023-10-31"})
        if os.environ.get("ELASTIC_API_KEY"):
            self.session.headers["Authorization"] = "ApiKey " + os.environ["ELASTIC_API_KEY"]
        else:
            self.session.auth = (env("ELASTIC_USERNAME"), env("ELASTIC_PASSWORD"))
        if insecure:
            self.session.verify = False
            requests.packages.urllib3.disable_warnings()
        elif os.environ.get("ELASTIC_CA_CERT"):
            self.session.verify = os.environ["ELASTIC_CA_CERT"]

    def call(self, method, url, body=None, ok=(200, 201), allow=()):
        resp = self.session.request(method, url, data=None if body is None else json.dumps(body),
                                    timeout=300)
        if resp.status_code in allow:
            return resp
        if resp.status_code not in ok:
            sys.exit(f"{method} {url} -> {resp.status_code}\n{resp.text[:2000]}")
        return resp

    def kbn(self, method, path, body=None, **kw):
        return self.call(method, self.kbn_base + path, body, **kw)

    def esr(self, method, path, body=None, **kw):
        if not self.es:
            sys.exit("ES_URL is required for the registry index")
        return self.call(method, self.es + path, body, **kw)


def env(name):
    val = os.environ.get(name)
    if not val:
        sys.exit(f"environment variable {name} is required")
    return val


def upsert(client, kind, obj, immutable):
    path = f"/api/agent_builder/{kind}/{obj['id']}"
    exists = client.kbn("GET", path, ok=(200,), allow=(404,)).status_code == 200
    if exists:
        client.kbn("PUT", path, update_body(obj, immutable))
        print(f"  updated {kind[:-1]} {obj['id']}")
    else:
        client.kbn("POST", f"/api/agent_builder/{kind}", obj)
        print(f"  created {kind[:-1]} {obj['id']}")


def generated_tool_ids(client, prefix):
    data = client.kbn("GET", "/api/agent_builder/tools").json()
    return [t["id"] for t in data.get("results", [])
            if t["id"].startswith(prefix + ".") and GENERATED_TAG in t.get("tags", [])]


def write_registry(client, index, docs):
    client.esr("DELETE", f"/{index}", ok=(200,), allow=(404,))
    client.esr("PUT", f"/{index}", REGISTRY_MAPPING)
    bulk = "".join(json.dumps({"index": {"_id": d["app_id"]}}) + "\n" + json.dumps(d) + "\n"
                   for d in docs)
    resp = client.session.post(f"{client.es}/{index}/_bulk?refresh=true", data=bulk,
                               headers={"Content-Type": "application/x-ndjson"}, timeout=60)
    if resp.status_code != 200 or resp.json().get("errors"):
        sys.exit(f"registry bulk failed: {resp.text[:2000]}")
    print(f"  registry index '{index}' written ({len(docs)} apps)")


def apply(client, bundle, agent_cfg):
    prefix = agent_cfg["tool_prefix"]
    print("registry:")
    write_registry(client, agent_cfg["registry_index"], bundle["registry"])
    print("tools:")
    wanted = {t["id"] for t in bundle["tools"]}
    for tool in bundle["tools"]:
        upsert(client, "tools", tool, immutable=("id", "type"))
    print("agent:")
    upsert(client, "agents", bundle["agent"], immutable=("id",))
    stale = [i for i in generated_tool_ids(client, prefix) if i not in wanted]
    for tool_id in stale:
        client.kbn("DELETE", f"/api/agent_builder/tools/{tool_id}")
        print(f"  deleted stale tool {tool_id}")
    print("done. Open Kibana > Agents and pick "
          f"'{bundle['agent']['name']}'.")


def test(client, bundle, lookback):
    failures = 0
    for tool in bundle["tools"]:
        params = {}
        if tool["type"] == "esql" and "lookback_minutes" in tool["configuration"]["params"]:
            params["lookback_minutes"] = lookback
        if tool["type"] == "index_search":
            params["query"] = "error"
        resp = client.kbn("POST", "/api/agent_builder/tools/_execute",
                          {"tool_id": tool["id"], "tool_params": params},
                          ok=(200,), allow=(400, 404, 500))
        text = resp.text
        bad = resp.status_code != 200 or '"type":"error"' in text.replace(" ", "")
        failures += bad
        rows = ""
        try:
            for res in resp.json().get("results", []):
                vals = res.get("data", {}).get("values")
                if vals is not None:
                    rows = f"{len(vals)} rows"
        except ValueError:
            pass
        print(f"{'FAIL' if bad else 'ok  '} {tool['id']:55} {rows}")
        if bad:
            print("      " + text[:600].replace("\n", " "))
    print(f"\n{len(bundle['tools']) - failures}/{len(bundle['tools'])} tools ok")
    return failures


def ask(client, agent_id, question):
    resp = client.kbn("POST", "/api/agent_builder/converse",
                      {"agent_id": agent_id, "input": question})
    data = resp.json()
    for step in data.get("steps", []):
        if step.get("type") == "tool_call":
            print(f"[tool] {step.get('tool_id')} {json.dumps(step.get('params', {}))}")
    print()
    print(data.get("response", {}).get("message", json.dumps(data, indent=2)))


def destroy(client, bundle, agent_cfg):
    client.kbn("DELETE", f"/api/agent_builder/agents/{bundle['agent']['id']}",
               ok=(200,), allow=(404,))
    for tool_id in generated_tool_ids(client, agent_cfg["tool_prefix"]):
        client.kbn("DELETE", f"/api/agent_builder/tools/{tool_id}")
        print(f"deleted tool {tool_id}")
    client.esr("DELETE", f"/{agent_cfg['registry_index']}", ok=(200,), allow=(404,))
    print("agent, tools and registry removed")


# --------------------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["render", "apply", "test", "ask", "destroy"])
    p.add_argument("question", nargs="?", help="question for the 'ask' command")
    p.add_argument("--apps", default=ROOT / "config" / "apps.yaml")
    p.add_argument("--agent-config", default=ROOT / "config" / "agent.yaml")
    p.add_argument("--out", default=ROOT / "out", type=Path)
    p.add_argument("--lookback", type=int, default=60, help="minutes, for 'test'")
    p.add_argument("--insecure", action="store_true", help="skip TLS verification")
    args = p.parse_args()

    agent_cfg, apps = load_config(args.apps, args.agent_config)
    bundle = build_all(agent_cfg, apps)

    if args.command == "render":
        render(bundle, agent_cfg, args.out)
        return
    client = Client(agent_cfg, insecure=args.insecure)
    if args.command == "apply":
        apply(client, bundle, agent_cfg)
    elif args.command == "test":
        sys.exit(1 if test(client, bundle, args.lookback) else 0)
    elif args.command == "ask":
        if not args.question:
            sys.exit('usage: setup_agent.py ask "how healthy is kafka?"')
        ask(client, bundle["agent"]["id"], args.question)
    elif args.command == "destroy":
        destroy(client, bundle, agent_cfg)


if __name__ == "__main__":
    main()
