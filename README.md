# topicfoundry — forge event schemas & filters from ABIs (offline)

**topicfoundry** takes one or more ABI files and turns every **event** into:
- a computed **topic0** (`keccak("EventName(type1,type2,…)")`);
- clean **indexed vs data** columns;
- ready-to-paste **SQL DDL** for **PostgreSQL**, **BigQuery**, and **ClickHouse**;
- a **JSON Schema** per event + a combined manifest;
- a **CSV data dictionary** for docs/reviews;
- quick **eth_getLogs** filter stubs (topics arrays).

Perfect for data pipelines, analytics, PR reviews, or quick local experiments —
all **offline**.

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
