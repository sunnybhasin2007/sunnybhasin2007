#!/usr/bin/env python3
"""Deploy the automatic alert root-cause analysis (Elastic Workflows, 9.4).

  deploy     create the RCA results index (if missing) and create/update the workflow(s)
  run-now    trigger every shard once, without waiting for the schedule
  status     last executions + latest RCA results
  disable    stop the schedule (workflows kept)
  enable     start the schedule again

Settings are passed as flags (they fill the `consts:` block of
workflows/alert_rca.yaml), e.g.

  python setup_alert_rca.py deploy --alert-index alert_details --component-field component \
      --agent-id component-agent --shards 4 --kibana-url https://kibana.mycorp.local:5601

--shards N deploys N copies of the workflow; each handles the components whose
name hashes to its shard, so N RCAs can run in parallel and all alerts of one
component still go to the same copy (grouping keeps working).

Connection: ES_URL, KIBANA_URL, ELASTIC_API_KEY (or ELASTIC_USERNAME/ELASTIC_PASSWORD),
ELASTIC_CA_CERT. The workflows run with the privileges of the user who deploys them.
"""
import argparse
import re
import sys

from apphealth_lib import ROOT, Client, load_settings

WORKFLOW_FILE = ROOT / "workflows" / "alert_rca.yaml"
PREFIX = "alert-rca"

RCA_INDEX_BODY = {
    # lookup mode lets the workflow's ES|QL LOOKUP JOIN skip alerts already handled
    "settings": {"index": {"mode": "lookup"}},
    "mappings": {"dynamic": False, "properties": {
        "alert_id": {"type": "keyword"}, "alert_index": {"type": "keyword"},
        "alert_time": {"type": "date"}, "alert_source": {"type": "text"},
        "alert_component": {"type": "keyword"},
        "component": {"type": "keyword"}, "rca_status": {"type": "keyword"},
        "case_id": {"type": "keyword"}, "case_url": {"type": "keyword"},
        "grouped_into_alert_id": {"type": "keyword"},
        "summary": {"type": "text"}, "root_cause": {"type": "text"}, "evidence": {"type": "text"},
        "still_ongoing": {"type": "boolean"}, "ongoing_evidence": {"type": "text"},
        "confidence": {"type": "keyword"}, "severity": {"type": "keyword"},
        "recommended_actions": {"type": "text"}, "indices_checked": {"type": "text"},
        "conversation_id": {"type": "keyword"}, "attempts": {"type": "integer"},
        "claimed_at": {"type": "date"}, "processed_at": {"type": "date"},
        "workflow_execution_id": {"type": "keyword"}, "error": {"type": "text"},
    }},
}

CONST_FLAGS = [  # flags that map 1:1 onto consts keys
    "alert_index", "alert_time_field", "component_field", "rca_index", "components_index",
    "lookback_minutes", "batch_size", "group_window", "kibana_url",
]


def set_const(text, key, val):
    new, n = re.subn(rf"^(  {key}: )\S.*?(\s+#.*)?$", lambda m: f"{m.group(1)}{quote(val)}{m.group(2) or ''}",
                     text, count=1, flags=re.M)
    if n != 1:
        sys.exit(f"could not set consts.{key} in {WORKFLOW_FILE}")
    return new


def render_yaml(a, shard, shards):
    text = WORKFLOW_FILE.read_text()
    for key in CONST_FLAGS:
        if getattr(a, key) is not None:
            text = set_const(text, key, getattr(a, key))
    text = set_const(text, "shard_index", shard)
    text = set_const(text, "shard_count", shards)
    text = re.sub(r"^(    key: )alert-rca-shard-\d+", rf"\g<1>alert-rca-shard-{shard}", text, count=1, flags=re.M)
    if shards > 1:
        text = re.sub(r"^name: (.+)$", rf"name: \1 (shard {shard + 1} of {shards})", text, count=1, flags=re.M)
    if a.agent_id:
        text = re.sub(r"^(\s*agent-id: ).+$", lambda m: m.group(1) + a.agent_id, text, flags=re.M)
    if a.connector_id:  # otherwise the space's default GenAI connector is used
        text = re.sub(r"^(\s*)agent-id: (.+)$", lambda m: f"{m.group(0)}\n{m.group(1)}connector-id: {a.connector_id}",
                      text, flags=re.M)
    if re.search(r"^  component_field: CHANGE_ME", text, flags=re.M):
        sys.exit("set --component-field (the alert field that holds the component name)")
    return text


def quote(val):
    val = str(val)
    return val if re.fullmatch(r"[\w.\-/:]+", val) and not val.startswith("@") else "'" + val.replace("'", "''") + "'"


def const_value(text, key):
    m = re.search(rf"^  {key}: (\S+)", text, flags=re.M)
    return m.group(1).strip("'") if m else None


def active_workflows(c):
    """{shard_index: workflow_id} of deployed (not deleted) RCA workflows.

    Kibana soft-deletes workflows: GET by id still answers for a deleted one, but only
    the list endpoint reflects what is really active, so look them up there.
    """
    found = c.kbn("GET", "/api/workflows?size=500").json().get("results", [])
    out = {}
    for w in found:
        if w["id"] == PREFIX or w["id"].startswith(PREFIX + "-"):
            yaml_text = c.kbn("GET", f"/api/workflows/workflow/{w['id']}").json().get("yaml", "")
            out[int(const_value(yaml_text, "shard_index") or 0)] = w["id"]
    return out


def free_id(c, shard):
    """A deleted workflow's id cannot be reused, so pick alert-rca-s0, alert-rca-s0-r2, ..."""
    for n in range(1, 100):
        wid = f"{PREFIX}-s{shard}" + ("" if n == 1 else f"-r{n}")
        if c.kbn("GET", f"/api/workflows/workflow/{wid}", ok=(200,), allow=(404,)).status_code == 404:
            return wid
    sys.exit("no free workflow id found")


def rca_index_name(a):
    return a.rca_index or const_value(WORKFLOW_FILE.read_text(), "rca_index")


def deploy(c, a):
    index = rca_index_name(a)
    if c.esr("HEAD", f"/{index}", ok=(200,), allow=(404,)).status_code == 404:
        c.esr("PUT", f"/{index}", RCA_INDEX_BODY)
        print(f"index {index}: created (lookup mode)")
    else:
        c.esr("PUT", f"/{index}/_mapping", RCA_INDEX_BODY["mappings"])
        print(f"index {index}: exists (mapping updated)")

    active = active_workflows(c)
    for shard in range(a.shards):
        yaml_text = render_yaml(a, shard, a.shards)
        wid = active.pop(shard, None)
        if wid:
            c.kbn("PUT", f"/api/workflows/workflow/{wid}", {"yaml": yaml_text})
            action = "updated"
        else:
            wid = free_id(c, shard)
            c.kbn("POST", "/api/workflows/workflow", {"yaml": yaml_text, "id": wid})
            action = "created"
        wf = c.kbn("GET", f"/api/workflows/workflow/{wid}").json()
        if not wf.get("valid"):
            sys.exit(f"workflow {wid} {action} but Kibana marked it INVALID - open Kibana > Workflows > "
                     f"'{wf.get('name')}' to see the error")
        print(f"workflow {wid}: {action}, valid, enabled={wf.get('enabled')}")
    for shard, wid in sorted(active.items()):  # shards no longer wanted
        c.kbn("DELETE", f"/api/workflows/workflow/{wid}", ok=(200, 204))
        print(f"workflow {wid}: deleted (shard {shard} no longer used)")
    print("Each runs every minute. Watch them in Kibana > Workflows > 'Alert root-cause analysis'.")


def require_workflows(c):
    active = active_workflows(c)
    if not active:
        sys.exit("the RCA workflow is not deployed - run: python setup_alert_rca.py deploy ...")
    return [active[k] for k in sorted(active)]


def set_enabled(c, enabled):
    for wid in require_workflows(c):
        c.kbn("PUT", f"/api/workflows/workflow/{wid}", {"enabled": enabled})
        print(f"workflow {wid}: {'enabled' if enabled else 'disabled'}")


def status(c, a):
    for wid in require_workflows(c):
        ex = c.kbn("GET", f"/api/workflows/workflow/{wid}/executions").json().get("results", [])
        print(f"workflow {wid} - last executions:")
        for e in ex[:5]:
            print(f"  {e.get('startedAt', '')[:19]}  {e.get('status', ''):10} {e.get('triggeredBy', '')}")
    rows = c.esr("POST", f"/{rca_index_name(a)}/_search", {
        "size": 15, "sort": [{"alert_time": "desc"}],
        "_source": ["alert_time", "component", "rca_status", "case_url", "still_ongoing", "summary", "error"]}
    ).json()["hits"]["hits"]
    print("\nlatest RCA results:")
    for h in rows:
        s = h["_source"]
        detail = s.get("summary") or (s.get("error") or "")[:80]
        print(f"  {s.get('alert_time', '')[:19]}  {s.get('component', '-'):16} {s.get('rca_status', ''):12} "
              f"ongoing={s.get('still_ongoing', '-'):5} {detail}  {s.get('case_url', '')}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["deploy", "run-now", "status", "disable", "enable"])
    p.add_argument("--alert-index", help="index pattern behind the alert data view (default alert_details)")
    p.add_argument("--alert-time-field", help="alert time field (default @timestamp)")
    p.add_argument("--component-field", help="alert field holding the component name (required on deploy)")
    p.add_argument("--components-index", help="component table (default component-registry)")
    p.add_argument("--agent-id", help="Agent Builder agent that does the RCA (default: as in the yaml)")
    p.add_argument("--shards", type=int, default=1, help="parallel copies of the workflow (default 1)")
    p.add_argument("--rca-index", help="results index (default alert-rca)")
    p.add_argument("--lookback-minutes", type=int, help="only alerts newer than this (default 120)")
    p.add_argument("--batch-size", type=int, help="alerts per run and shard (default 5)")
    p.add_argument("--group-window", help="e.g. now-5m: repeat alerts in this window join the open case")
    p.add_argument("--kibana-url", help="public Kibana URL for links in cases")
    p.add_argument("--connector-id", help="LLM connector id (default: the space's default AI connector)")
    p.add_argument("--insecure", action="store_true")
    a = p.parse_args()
    if a.shards < 1:
        sys.exit("--shards must be >= 1")
    c = Client(load_settings(), insecure=a.insecure, need_kibana=True)
    if a.command == "deploy":
        deploy(c, a)
    elif a.command == "run-now":
        for wid in require_workflows(c):
            r = c.kbn("POST", f"/api/workflows/workflow/{wid}/run", {"inputs": {}}).json()
            print(f"{wid}: started execution {r.get('workflowExecutionId')}")
    elif a.command == "status":
        status(c, a)
    elif a.command in ("disable", "enable"):
        set_enabled(c, a.command == "enable")


if __name__ == "__main__":
    main()
