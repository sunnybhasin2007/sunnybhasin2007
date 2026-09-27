# App Health Agent for Kibana Agent Builder (Elastic 9.4)

An agent for the Kibana **Agent Builder** chat. You ask things like
*"how healthy is Kafka?"*, *"any issues in payments in the last 4 hours?"* or
*"root cause of the Kafka errors since 10:00"*. The agent works out which
application you mean, looks up **that application's indices**, and runs checks
only against those indices. It never searches the whole cluster.

## How it works

```
User: "is kafka ok?"
   │
   ▼
App Health Agent  (custom instructions: resolve app → only use its tools)
   │ 1. apphealth.list_applications ──► index "app-health-registry"
   │        kafka → logs-kafka.log-*, metrics-kafka.*-*, depends_on: zookeeper
   │ 2. apphealth.kafka.health_summary      (ES|QL, logs-kafka.log-* only)
   │ 3. apphealth.kafka.metrics_freshness   (ES|QL, metrics-kafka.*-* only)
   │ 4. apphealth.kafka.consumer_lag        (custom check from apps.yaml)
   │ 5. apphealth.kafka.find_errors         (if errors > 0)
   ▼
"DEGRADED: broker-2 silent for 23 min, 4.1% error rate, consumer group X lag 1.2M..."
```

| Piece | What it is |
|---|---|
| `config/apps.yaml` | **The catalog.** Maps each app → its log/metric indices, aliases, owner, dependencies, field names, and custom checks. This is the file you edit. |
| `config/agent.yaml` | Agent name/id, Kibana space, tool prefix, extra built-in tools. |
| `app-health-registry` index | Written from `apps.yaml`. The agent reads it to map a name ("zk", "kafka brokers") to an app and its indices. |
| Generated tools (per app) | `health_summary`, `find_errors`, `error_timeline`, `search_logs` (index-scoped search), `metrics_freshness`, plus one tool per custom `check`. Every tool has its index pattern **hard-coded**, so it can only read that app's data. |
| Agent | Instructions with three workflows (health check, find issues, root cause), including checking upstream `depends_on` apps during root-cause analysis. |

## Setup

### 1. Prerequisites
- Elastic 9.4 with Agent Builder enabled and an LLM connector (you already have this).
- A user or API key that can manage Agent Builder tools/agents and create an
  index (`app-health-registry`). A superuser is fine for the first setup.
- Python 3.9+ on any machine that can reach Kibana and Elasticsearch:
  `pip install -r requirements.txt`
  *(No Python on the network? Use option B in step 3.)*

### 2. Describe your applications in `config/apps.yaml`
For each app, fill in `log_indices` / `metric_indices` with the patterns you
actually use (check in **Discover** or `GET _cat/indices/*kafka*?v`), and add aliases.
If your logs are not ECS, override `host_field`, `message_field` or
`error_condition` for that app (see the `payments-api` example).

The Kafka example assumes the Elastic **Kafka integration** data streams
(`logs-kafka.log-*`, `metrics-kafka.consumergroup-*`, `metrics-kafka.partition-*`).
If you ship Kafka data differently, change the index patterns and field names
in its `checks`.

### 3. Deploy

**Option A: script (recommended)**
```bash
export KIBANA_URL=https://kibana.mycorp.local:5601
export ES_URL=https://es.mycorp.local:9200
export ELASTIC_API_KEY=<base64 id:key>      # or ELASTIC_USERNAME / ELASTIC_PASSWORD
export ELASTIC_CA_CERT=/path/to/ca.crt      # on-prem self-signed CA

python setup_agent.py render   # optional: review ./out/*.json first
python setup_agent.py apply    # registry + tools + agent (idempotent; removes stale tools)
python setup_agent.py test     # runs every tool once and prints any ES|QL / field errors
python setup_agent.py ask "What is the health of kafka?"
```

**Option B: Kibana Dev Tools, no network access from a script**
```bash
python setup_agent.py render      # can run on a laptop, no cluster access needed
```
Paste `out/devtools_console.txt` into **Kibana → Dev Tools → Console** and run it
top to bottom.

### 4. Use it
Kibana → **Agents** (Agent Builder) → choose **App Health Agent** → ask:

- `health check of kafka`
- `any issues with zookeeper in the last 4 hours?`
- `why are payments failing? find the root cause`
- `show kafka consumer lag for the last day`
- `which apps can you check?`

### 5. (Recommended) Enforce the index boundary with a role
Every generated tool can only read its own index patterns. The optional built-in
tools `platform.core.execute_esql` and `get_index_mapping` (used for deeper RCA)
are held to the registered indices by the agent's instructions only, and tools
run with the chat user's own permissions. For a hard guarantee, have chat users
use the role at the end of `out/devtools_console.txt` (`app_health_agent_user`:
read-only on the registered indices). Add the Kibana privileges for Agent Builder
and Connectors to that role in **Stack Management → Roles**.
You can also remove those two tools from `extra_platform_tools` in `agent.yaml`.

## Adding a new application
1. Add a block to `config/apps.yaml` (id, aliases, indices, optional `checks`).
2. Run `python setup_agent.py apply && python setup_agent.py test`.

A custom check is any ES|QL query. These placeholders are filled in for you:
`{time_filter}` (driven by the `lookback_minutes` parameter the agent passes),
`{ts}`, `{logs}` and `{metrics}`.

```yaml
checks:
  - id: under_replicated_partitions
    description: Brokers reporting under-replicated partitions.
    query: |
      FROM metrics-kafka.broker-*
      | WHERE {time_filter}
      | STATS max_urp = MAX(kafka.broker.<your_urp_field>) BY host.name
      | WHERE max_urp > 0
      | LIMIT 20
```

## Troubleshooting
- **`test` shows `Unknown index`**: the index pattern in `apps.yaml` matches nothing.
- **`Unknown column [log.level]`**: that app's logs don't have the field. Override
  `error_condition` or `warning_condition` for the app, e.g.
  `'message LIKE "*ERROR*"'`.
- **`CATEGORIZE` errors**: set `error_grouping: truncate` for that app.
- **Non-default Kibana space**: set `kibana_space` in `agent.yaml`.
- **Agent answers about other indices**: remove `platform.core.execute_esql`
  from `extra_platform_tools` and apply the role from step 5.
- `python setup_agent.py destroy` removes the agent, the generated tools and the registry index.
