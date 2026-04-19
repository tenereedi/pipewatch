# pipewatch

Lightweight CLI to monitor and alert on data pipeline health across multiple sources.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Define your pipeline sources in a config file (`pipewatch.yaml`), then run:

```bash
pipewatch monitor --config pipewatch.yaml
```

**Example config:**

```yaml
pipelines:
  - name: daily_sales_etl
    type: postgres
    query: "SELECT COUNT(*) FROM orders WHERE created_at::date = CURRENT_DATE"
    threshold:
      min: 100
    alert: slack

  - name: user_events_kafka
    type: kafka
    topic: user-events
    lag_threshold: 5000
    alert: email
```

**Run a one-time health check:**

```bash
pipewatch check --pipeline daily_sales_etl
```

**Watch continuously with an interval:**

```bash
pipewatch monitor --interval 60
```

**Output example:**

```
[OK]   daily_sales_etl     rows=1482   threshold=min:100
[WARN] user_events_kafka   lag=7821    threshold=max:5000  → alert sent
```

---

## Supported Sources

- PostgreSQL / MySQL
- Kafka
- AWS S3
- REST APIs

---

## License

MIT © 2024 [yourname](https://github.com/yourname)