#!/usr/bin/env python3
"""Deploy the App Health Agent to Kibana Agent Builder (Elastic 9.4).

The agent has three small tools that read the component table
(see components.py) plus the built-in ES|QL tool. It never gets per-application
tools, so adding applications only means adding rows to the table.

  render   write out/ (tools, agent, instructions) + out/devtools_console.txt; no network
  apply    create/update the tools and the agent in Kibana
  test     execute the agent's table tools once through Kibana
  ask      ask the agent a question:  ask "how healthy is kafka?"
  destroy  delete the agent and its tools (the component table is kept)

Connection: KIBANA_URL, ES_URL, ELASTIC_API_KEY (or ELASTIC_USERNAME/ELASTIC_PASSWORD),
ELASTIC_CA_CERT.
"""
import argparse
import copy
import json
import sys
from pathlib import Path

from apphealth_lib import (CHECKS_MAPPING, ROOT, Client, components_mapping, load_settings,
                           pipeline_body, template_purposes)

TAG = "app-health"


def build_tools(s):
    p, comp, checks = s["tool_prefix"], s["components_index"], s["checks_index"]
    component_param = {"component": {
        "type": "string",
        "description": "The component id exactly as returned by list_components (lowercase).",
    }}
    return [
        {
            "id": f"{p}.list_components",
            "type": "esql",
            "description": (
                "ALWAYS CALL FIRST. Lists every application/component in the component "
                "table: id, display name, aliases, owner, dependencies and which signals "
                "(logs/metrics/traces) it has. Use it to map the user's wording "
                "(e.g. 'kafka brokers', 'zk', 'payments') to a component id."),
            "tags": [TAG],
            "configuration": {"query": f"""FROM {comp}
| EVAL signals = TRIM(CONCAT(
    CASE(MV_COUNT(log_indices) > 0, "logs ", ""),
    CASE(MV_COUNT(metric_indices) > 0, "metrics ", ""),
    CASE(MV_COUNT(trace_indices) > 0, "traces", "")))
| KEEP component, display_name, aliases, description, owner, depends_on, signals
| SORT component
| LIMIT 1000""", "params": {}},
        },
        {
            "id": f"{p}.get_component",
            "type": "esql",
            "description": (
                "Returns one component's row: its log/metric/trace indices, filters, "
                "field names (resolved.*) and its READY-TO-RUN ES|QL queries (queries.*). "
                "Call after list_components, and for each depends_on component during "
                "root-cause analysis."),
            "tags": [TAG],
            "configuration": {"query": f"""FROM {comp}
| WHERE component == TO_LOWER(?component)
| KEEP component, display_name, description, owner, depends_on, schema, service_name,
       log_indices, metric_indices, trace_indices, notes, resolved.*, queries.*
| LIMIT 1""", "params": copy.deepcopy(component_param)},
        },
        {
            "id": f"{p}.get_component_checks",
            "type": "esql",
            "description": (
                "Returns the custom ES|QL health checks registered for one component "
                "(e.g. Kafka consumer lag). Run each one during a health check."),
            "tags": [TAG],
            "configuration": {"query": f"""FROM {checks}
| WHERE component == TO_LOWER(?component)
| KEEP check_id, description, query
| SORT check_id
| LIMIT 50""", "params": copy.deepcopy(component_param)},
        },
    ]


INSTRUCTIONS = """\
You are the App Health Agent. You answer questions about the health, issues and
root cause of problems of specific applications/components (Kafka, ZooKeeper,
services...) using ONLY the indices registered for that component in the
component table. Its logs, metrics and traces come from OpenTelemetry (or ECS).

## Hard rules
1. Never search the whole cluster. Never query an index that is not in the
   component's log_indices, metric_indices or trace_indices.
2. Always start with `{p}.list_components` and map the user's wording to exactly
   one component id (match id, display_name or aliases, case-insensitive).
   No match: say so and list the available components. Several matches: ask.
3. Then call `{p}.get_component` for that id. It returns ready-to-run ES|QL in
   `queries.<name>` and custom checks via `{p}.get_component_checks`.
4. Run those queries with `platform.core.execute_esql`, copying them EXACTLY and
   only replacing the placeholders:
     {{MINUTES}}  lookback in minutes (default {lookback}; "last 4 hours" -> 240,
                "today"/"last day" -> 1440, "last week" -> 10080)
     {{TEXT}}     words to search for (exception class, id, topic, host...)
     {{TRACE_ID}} a trace id taken from trace_errors / log results
   Only write your own ES|QL if no stored query fits; then use the same indices,
   the component's filter from `resolved.<signal>_filter`, field names from
   `resolved.*`, a time filter and a LIMIT. Use `platform.core.get_index_mapping`
   only on the component's own indices.
5. Base every statement on query results; quote numbers, hosts, timestamps.
   A query error (unknown index/column) or zero rows is a finding: report it
   (e.g. "no logs from kafka in the last 60 minutes - it may be down or not shipping").
6. Run independent queries in parallel where possible and keep answers concise.

## Stored queries (a null queries.<name> means the component lacks that signal)
{purposes}

## Workflows
**Health check** ("health of X", "is X ok", "status of X"):
 - Run log_health, metric_freshness, trace_summary (whichever exist) and every
   custom check. If errors > 0 also run log_errors (and trace_errors).
 - Answer: status HEALTHY / DEGRADED / DOWN with a one-line reason, a small table
   of key numbers per host/operation, top issues, and recommended next steps.
 - DOWN = no data in the window, or every host silent > 15 min.
   DEGRADED = error rate > 5%, any host silent > 15 min (minutes_since_last_event),
   stale metrics, trace error rate > 5% or p95 clearly abnormal, or a failing
   custom check. Otherwise HEALTHY.

**Find issues** ("any issues/errors/problems in X"):
 - Run log_errors and trace_errors, then log_timeline for the same window.
 - List issues ranked by impact: occurrences, affected hosts, ongoing or resolved.

**Root cause analysis** ("why is X failing/slow", "root cause", "what happened"):
 1. log_timeline / trace_timeline: find when errors, volume or latency changed (onset).
 2. log_errors / trace_errors over a window starting shortly before the onset;
    identify the earliest new error pattern.
 3. log_search for that pattern, and log_by_trace for an example_trace_id,
    to get concrete evidence.
 4. Custom checks and metric_freshness for the same window.
 5. For each component in depends_on: get_component, then log_health and
    log_errors for the same window. An upstream problem that began first is a
    likely cause. (Also mention components that depend on X, from list_components,
    if they are affected.)
 6. Report: timeline of events, most likely root cause with evidence, confidence
    (high/medium/low), alternatives, and concrete next steps. Separate evidence
    from hypothesis.
"""


def build_agent(s):
    p = s["tool_prefix"]
    purposes = "\n".join(f"- `{name}`: {text}" for name, text in template_purposes(s).items())
    a = s["agent"]
    tool_ids = [t["id"] for t in build_tools(s)] + s.get("platform_tools", [])
    return {
        "id": a["id"],
        "name": a["name"],
        "description": a["description"].strip(),
        "labels": a.get("labels", []),
        "avatar_color": a.get("avatar_color"),
        "avatar_symbol": a.get("avatar_symbol"),
        "configuration": {
            "instructions": INSTRUCTIONS.format(p=p, lookback=s.get("default_lookback_minutes", 60),
                                                purposes=purposes),
            "tools": [{"tool_ids": tool_ids}],
        },
    }


# --------------------------------------------------------------------------- render
def render(s, out):
    out.mkdir(parents=True, exist_ok=True)
    tools, agent = build_tools(s), build_agent(s)
    (out / "tools.json").write_text(json.dumps(tools, indent=2))
    (out / "agent.json").write_text(json.dumps(agent, indent=2))
    (out / "agent_instructions.md").write_text(agent["configuration"]["instructions"])

    space = s.get("kibana_space", "default")
    kbn = "kbn:" if space == "default" else f"kbn:/s/{space}"
    idx, chk = s["components_index"], s["checks_index"]
    comp_body = components_mapping()
    comp_body["settings"] = {"index": {"default_pipeline": f"{idx}-render"}}
    L = [
        "# Generated by setup_agent.py render - paste into Kibana > Dev Tools > Console.",
        "# Run the sections in order. POST of an existing tool/agent returns 409:",
        "# use the commented PUT instead (body without id/type).",
        "",
        "# ---- 1. Pipeline that renders each component's queries",
        f"PUT _ingest/pipeline/{idx}-render", json.dumps(pipeline_body(s), indent=2), "",
        "# ---- 2. Component table + checks table (skip if they already exist)",
        f"PUT {idx}", json.dumps(comp_body, indent=2), "",
        f"PUT {chk}", json.dumps(CHECKS_MAPPING, indent=2), "",
        "# ---- 3. Add components (one PUT per component; re-PUT to change it)",
        f"PUT {idx}/_doc/kafka?refresh=true",
        json.dumps({"component": "kafka", "display_name": "Apache Kafka",
                    "aliases": ["kafka cluster", "brokers"], "service_name": "kafka",
                    "log_indices": ["logs-*.otel-*"], "metric_indices": ["metrics-*.otel-*"],
                    "depends_on": ["zookeeper"], "owner": "messaging-team"}, indent=2), "",
        f"GET {idx}/_doc/kafka   # check the rendered queries", "",
        "# Custom check example ({MINUTES} is filled in by the agent)",
        f"PUT {chk}/_doc/kafka__consumer_lag?refresh=true",
        json.dumps({"component": "kafka", "check_id": "consumer_lag",
                    "description": "Consumer lag per group/topic",
                    "query": "FROM metrics-*.otel-*\n| WHERE @timestamp >= NOW() - {MINUTES} minutes "
                             "AND resource.attributes.service.name == \"kafka\"\n"
                             "| STATS max_lag = MAX(metrics.kafka.consumer_group.lag) "
                             "BY attributes.group, attributes.topic\n| SORT max_lag DESC\n| LIMIT 25"},
                   indent=2), "",
        "# ---- 4. Agent Builder tools",
    ]
    for t in tools:
        L += [f"POST {kbn}/api/agent_builder/tools", json.dumps(t, indent=2),
              f"# PUT {kbn}/api/agent_builder/tools/{t['id']}", ""]
    L += ["# ---- 5. Agent", f"POST {kbn}/api/agent_builder/agents", json.dumps(agent, indent=2),
          f"# PUT {kbn}/api/agent_builder/agents/{agent['id']}", ""]
    (out / "devtools_console.txt").write_text("\n".join(L))
    print(f"wrote {out}/devtools_console.txt, tools.json, agent.json, agent_instructions.md")


# --------------------------------------------------------------------------- kibana
def upsert(c, kind, obj, immutable):
    path = f"/api/agent_builder/{kind}/{obj['id']}"
    exists = c.kbn("GET", path, ok=(200,), allow=(404,)).status_code == 200
    body = {k: v for k, v in obj.items() if not (exists and k in immutable)}
    if exists:
        c.kbn("PUT", path, body)
    else:
        c.kbn("POST", f"/api/agent_builder/{kind}", body)
    print(f"  {'updated' if exists else 'created'} {kind[:-1]} {obj['id']}")


def apply(c, s):
    for idx in (s["components_index"], s["checks_index"]):
        if c.esr("HEAD", f"/{idx}", ok=(200,), allow=(404,)).status_code == 404:
            print(f"warning: index {idx} does not exist yet - run `python components.py init`",
                  file=sys.stderr)
    for tool in build_tools(s):
        upsert(c, "tools", tool, immutable=("id", "type"))
    agent = build_agent(s)
    upsert(c, "agents", agent, immutable=("id",))
    print(f"done. Kibana > Agents > '{agent['name']}'")


def test(c, s):
    rows = c.esr("POST", f"/{s['components_index']}/_search",
                 {"size": 1, "sort": [{"component": "asc"}]}).json()["hits"]["hits"]
    component = rows[0]["_source"]["component"] if rows else "kafka"
    fails = 0
    for tool in build_tools(s):
        params = {"component": component} if tool["configuration"]["params"] else {}
        resp = c.kbn("POST", "/api/agent_builder/tools/_execute",
                     {"tool_id": tool["id"], "tool_params": params}, ok=(200,), allow=(400, 404, 500))
        results = resp.json().get("results", []) if resp.status_code == 200 else []
        error = resp.status_code != 200 or any(r.get("type") == "error" for r in results)
        rows = [len(r["data"]["values"]) for r in results if isinstance(r.get("data"), dict)
                and "values" in r["data"]]
        fails += error
        print(f"{'FAIL' if error else 'ok  '} {tool['id']:32} {params} "
              f"{str(rows[0]) + ' rows' if rows else ''}")
        if error:
            print("     ", resp.text[:800])
    return fails


def ask(c, s, question):
    data = c.kbn("POST", "/api/agent_builder/converse",
                 {"agent_id": s["agent"]["id"], "input": question}).json()
    for step in data.get("steps", []):
        if step.get("type") == "tool_call":
            params = json.dumps(step.get("params", {}))
            print(f"[tool] {step.get('tool_id')} {params[:300]}")
    print("\n" + data.get("response", {}).get("message", json.dumps(data, indent=2)))


def destroy(c, s):
    c.kbn("DELETE", f"/api/agent_builder/agents/{s['agent']['id']}", ok=(200,), allow=(404,))
    for tool in build_tools(s):
        c.kbn("DELETE", f"/api/agent_builder/tools/{tool['id']}", ok=(200,), allow=(404,))
    print("agent and tools deleted (component table kept; delete the indices yourself if wanted)")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["render", "apply", "test", "ask", "destroy"])
    p.add_argument("question", nargs="?")
    p.add_argument("--out", type=Path, default=ROOT / "out")
    p.add_argument("--insecure", action="store_true", help="skip TLS verification")
    a = p.parse_args()
    s = load_settings()
    if a.command == "render":
        render(s, a.out)
        return
    c = Client(s, insecure=a.insecure, need_kibana=True)
    if a.command == "apply":
        apply(c, s)
    elif a.command == "test":
        sys.exit(1 if test(c, s) else 0)
    elif a.command == "ask":
        if not a.question:
            sys.exit('usage: setup_agent.py ask "how healthy is kafka?"')
        ask(c, s, a.question)
    elif a.command == "destroy":
        destroy(c, s)


if __name__ == "__main__":
    main()
