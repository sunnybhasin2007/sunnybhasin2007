#!/usr/bin/env python3
"""Deploy the automatic alert root-cause analysis (Elastic Workflows, 9.4).

  deploy     create the RCA results index (if missing) and create/update the workflow
  run-now    trigger the workflow once, without waiting for the schedule
  status     last executions + latest RCA results
  disable    stop the schedule (workflow kept)
  enable     start the schedule again

Settings are passed as flags (they fill the `consts:` block of
workflows/alert_rca.yaml), e.g.

  python setup_alert_rca.py deploy --alert-index alerts-logstash \
      --alert-time-field @timestamp --kibana-url https://kibana.mycorp.local:5601

Connection: ES_URL, KIBANA_URL, ELASTIC_API_KEY (or ELASTIC_USERNAME/ELASTIC_PASSWORD),
ELASTIC_CA_CERT. The workflow runs with the privileges of the user who deploys it.
"""
import argparse
import json
import re
import sys

from apphealth_lib import ROOT, Client, load_settings

WORKFLOW_FILE = ROOT / "workflows" / "alert_rca.yaml"
WORKFLOW_ID = "alert-rca"

RCA_INDEX_BODY = {
    # lookup mode lets the workflow's ES|QL LOOKUP JOIN skip alerts already handled
    "settings": {"index": {"mode": "lookup"}},
    "mappings": {"dynamic": False, "properties": {
        "alert_id": {"type": "keyword"}, "alert_index": {"type": "keyword"},
        "alert_time": {"type": "date"}, "alert_source": {"type": "text"},
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

CONST_FLAGS = {  # flag -> consts key
    "alert_index": "alert_index", "alert_time_field": "alert_time_field",
    "rca_index": "rca_index", "lookback_minutes": "lookback_minutes",
    "batch_size": "batch_size", "group_window": "group_window", "kibana_url": "kibana_url",
}


def render_yaml(a):
    text = WORKFLOW_FILE.read_text()
    for flag, key in CONST_FLAGS.items():
        val = getattr(a, flag)
        if val is None:
            continue
        text, n = re.subn(rf"^(  {key}: )\S.*?(\s+#.*)?$", lambda m: f"{m.group(1)}{quote(val)}{m.group(2) or ''}",
                          text, count=1, flags=re.M)
        if n != 1:
            sys.exit(f"could not set consts.{key} in {WORKFLOW_FILE}")
    if a.connector_id:  # otherwise the space's default GenAI connector is used
        text = re.sub(r"^(\s*)agent-id: (.+)$", lambda m: f"{m.group(0)}\n{m.group(1)}connector-id: {a.connector_id}",
                      text, flags=re.M)
    if a.agent_id:
        text = re.sub(r"^(\s*agent-id: ).+$", lambda m: m.group(1) + a.agent_id, text, flags=re.M)
    return text


def quote(val):
    val = str(val)
    return val if re.fullmatch(r"[\w.\-/:]+", val) and not val.startswith("@") else "'" + val.replace("'", "''") + "'"


def rca_index_name(a):
    if a.rca_index:
        return a.rca_index
    m = re.search(r"^  rca_index: (\S+)", WORKFLOW_FILE.read_text(), flags=re.M)
    return m.group(1)


def active_workflow_id(c):
    """Id of the deployed (not deleted) RCA workflow, or None.

    Kibana soft-deletes workflows: GET by id still answers for a deleted one, but only
    the list endpoint reflects what is really active, so look it up there.
    """
    found = c.kbn("GET", "/api/workflows?size=500").json().get("results", [])
    ids = [w["id"] for w in found if w["id"] == WORKFLOW_ID or w["id"].startswith(WORKFLOW_ID + "-")]
    return ids[0] if ids else None


def free_workflow_id(c):
    """A deleted workflow's id cannot be reused, so pick alert-rca, alert-rca-2, ..."""
    for n in range(1, 100):
        wid = WORKFLOW_ID if n == 1 else f"{WORKFLOW_ID}-{n}"
        if c.kbn("GET", f"/api/workflows/workflow/{wid}", ok=(200,), allow=(404,)).status_code == 404:
            return wid
    sys.exit("no free workflow id found")


def require_workflow(c):
    wid = active_workflow_id(c)
    if not wid:
        sys.exit("the RCA workflow is not deployed - run: python setup_alert_rca.py deploy ...")
    return wid


def deploy(c, a):
    index = rca_index_name(a)
    if c.esr("HEAD", f"/{index}", ok=(200,), allow=(404,)).status_code == 404:
        c.esr("PUT", f"/{index}", RCA_INDEX_BODY)
        print(f"index {index}: created (lookup mode)")
    else:
        c.esr("PUT", f"/{index}/_mapping", RCA_INDEX_BODY["mappings"])
        print(f"index {index}: exists (mapping updated)")

    yaml_text = render_yaml(a)
    wid = active_workflow_id(c)
    if wid:
        c.kbn("PUT", f"/api/workflows/workflow/{wid}", {"yaml": yaml_text})
        action = "updated"
    else:
        wid = free_workflow_id(c)
        c.kbn("POST", "/api/workflows/workflow", {"yaml": yaml_text, "id": wid})
        action = "created"
        if wid != WORKFLOW_ID:
            print(f"note: id '{WORKFLOW_ID}' belongs to a deleted workflow, using '{wid}'")
    wf = c.kbn("GET", f"/api/workflows/workflow/{wid}").json()
    if not wf.get("valid"):
        sys.exit(f"workflow {action} but Kibana marked it INVALID - open Kibana > Workflows > "
                 f"'{wf.get('name')}' to see the error")
    print(f"workflow {wid}: {action}, valid, enabled={wf.get('enabled')}")
    print("It runs every minute. Watch it in Kibana > Workflows > 'Alert root-cause analysis' > Executions.")


def set_enabled(c, enabled):
    wid = require_workflow(c)
    c.kbn("PUT", f"/api/workflows/workflow/{wid}", {"enabled": enabled})
    print(f"workflow {wid}: {'enabled' if enabled else 'disabled'}")


def status(c, a):
    wid = require_workflow(c)
    ex = c.kbn("GET", f"/api/workflows/workflow/{wid}/executions").json().get("results", [])
    print(f"workflow {wid} - last executions:")
    for e in ex[:10]:
        print(f"  {e.get('startedAt', '')[:19]}  {e.get('status', ''):10} {e.get('triggeredBy', '')}")
    rows = c.esr("POST", f"/{rca_index_name(a)}/_search", {
        "size": 15, "sort": [{"alert_time": "desc"}],
        "_source": ["alert_time", "component", "rca_status", "case_url", "still_ongoing", "confidence",
                    "summary", "attempts", "error"]}).json()["hits"]["hits"]
    print("\nlatest RCA results:")
    for h in rows:
        s = h["_source"]
        detail = s.get("summary") or (s.get("error") or "")[:80]
        print(f"  {s.get('alert_time', '')[:19]}  {s.get('component', '-'):14} {s.get('rca_status', ''):12} "
              f"ongoing={s.get('still_ongoing', '-'):5} {detail}  {s.get('case_url', '')}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["deploy", "run-now", "status", "disable", "enable"])
    p.add_argument("--alert-index", help="index/data stream Logstash writes alerts to")
    p.add_argument("--alert-time-field", help="alert time field (default @timestamp)")
    p.add_argument("--rca-index", help="results index (default alert-rca)")
    p.add_argument("--lookback-minutes", type=int, help="only alerts newer than this (default 120)")
    p.add_argument("--batch-size", type=int, help="alerts per run (default 5)")
    p.add_argument("--group-window", help="e.g. now-30m: repeat alerts in this window join the open case")
    p.add_argument("--kibana-url", help="public Kibana URL for links in cases")
    p.add_argument("--agent-id", help="agent to use (default app-health-agent)")
    p.add_argument("--connector-id", help="LLM connector id (default: the space's default AI connector)")
    p.add_argument("--insecure", action="store_true")
    a = p.parse_args()
    c = Client(load_settings(), insecure=a.insecure, need_kibana=True)
    if a.command == "deploy":
        deploy(c, a)
    elif a.command == "run-now":
        r = c.kbn("POST", f"/api/workflows/workflow/{require_workflow(c)}/run", {"inputs": {}}).json()
        print(f"started execution {r.get('workflowExecutionId')} - check with: python setup_alert_rca.py status")
    elif a.command == "status":
        status(c, a)
    elif a.command in ("disable", "enable"):
        set_enabled(c, a.command == "enable")


if __name__ == "__main__":
    main()
