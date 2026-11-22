# Module 06: Write-Audit-Publish (WAP) & Zero-Downtime Swaps

![Data Quality Observability & Write-Audit-Publish (WAP) Circuit Breaker Flow](../../assets/whiteboards/data_quality_wap_quarantine_architecture.webp)
> *Architectural Blueprint: [Data Quality Observability & Write-Audit-Publish (WAP) Circuit Breaker Flow](../../docs/whiteboard_diagrams/README.md#data-quality-wap-quarantine). View full multi-persona breakdown and engine mechanisms in the master index.*

## 1. WAP Pattern Mechanics
1. **Write**: Pipeline writes transformed data to an isolated branch or staging table.
2. **Audit**: Automated quality assertion suites (row count, freshness, referential integrity, null constraints) validate the data.
3. **Publish**: If all audits pass, an atomic metadata commit merges the branch into the production table. If any audit fails, the branch is discarded with zero production impact.

---

## 2. Production Apache Iceberg WAP Implementation (SQL)

Apache Iceberg natively supports Write-Audit-Publish via table branches and the `system.publish_changes` procedure:

```sql
-- Step 1: Create an isolated audit branch
ALTER TABLE lakehouse.marts.fct_orders CREATE BRANCH audit_branch_run_20261121;

-- Step 2: Write incremental batch to the audit branch
INSERT INTO lakehouse.marts.fct_orders.branch_audit_branch_run_20261121
SELECT * FROM staging.stg_orders;

-- Step 3: Run Audit Assertions against the branch
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN order_total_usd < 0 THEN 1 ELSE 0 END) AS negative_totals
FROM lakehouse.marts.fct_orders.branch_audit_branch_run_20261121;

-- Step 4: Publish (Fast-forward main branch to the audit branch commit)
CALL lakehouse.system.fast_forward('marts.fct_orders', 'main', 'audit_branch_run_20261121');

-- Step 5: Clean up ephemeral audit branch
ALTER TABLE lakehouse.marts.fct_orders DROP BRANCH audit_branch_run_20261121;
```

---

## 3. Production Airflow 2/3 Dynamic Task Mapping WAP Pipeline

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import requests

default_args = {
    "owner": "dataops",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["wap", "dataops", "iceberg"]
)
def wap_orchestration_pipeline():

    @task
    def create_staging_branch(table_name: str) -> str:
        branch_id = f"audit_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        print(f"Creating isolated branch {branch_id} for {table_name}")
        return branch_id

    @task
    def write_staged_partitions(table_name: str, branch_id: str, partition_key: str):
        print(f"Writing {partition_key} to {table_name} on branch {branch_id}")
        return {"table": table_name, "branch": branch_id, "partition": partition_key}

    @task
    def audit_quality_gate(write_result: dict) -> bool:
        # Quality assertions: NULL checks, schema drift, threshold limits
        print(f"Auditing data on branch {write_result['branch']}")
        null_count = 0  # Simulated quality metric
        if null_count > 0:
            raise AirflowFailException(f"Quality gate breached on {write_result['table']}!")
        return True

    @task
    def atomic_publish_swap(table_name: str, branch_id: str):
        print(f"✅ Fast-forwarding main branch to {branch_id} on {table_name}")

    # Dynamic Task Mapping over partition slices
    partitions = ["2026-11-20", "2026-11-21", "2026-11-22"]
    branch = create_staging_branch("fct_orders")

    # Fan out writes across partitions
    staged_writes = write_staged_partitions.partial(
        table_name="fct_orders", branch_id=branch
    ).expand(partition_key=partitions)

    # Audit all partition writes
    audited = audit_quality_gate.expand(write_result=staged_writes)

    # Atomic swap once all audits succeed
    atomic_publish_swap("fct_orders", branch) << audited

wap_dag = wap_orchestration_pipeline()
```
