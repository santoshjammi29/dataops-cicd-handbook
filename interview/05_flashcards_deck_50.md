# 🗂️ The DataOps, CI/CD & Orchestration Handbook — Technical Flashcard Deck (50 Active Recall Questions)

> **Coverage**: Modules 01 through 10 of The DataOps, CI/CD & Orchestration Handbook
> **Focus**: 50 Rapid-Fire Questions & Answers for Airflow, GitOps, CI/CD, Terraform, and Testing

---

### 1. What is DataOps?

**Answer:**  
An automated, process-oriented methodology used by data teams to improve data quality and cycle time of data analytics.

---

### 2. What is GitOps in modern data engineering?

**Answer:**  
Using Git repositories as the single source of truth for declarative infrastructure, data pipelines, and deployment automation.

---

### 3. What is the Write-Audit-Publish (WAP) pattern?

**Answer:**  
Data is written to an isolated staging area/branch, audited with automated quality tests, and published to production only upon passing.

---

### 4. Difference between LocalExecutor, CeleryExecutor, and KubernetesExecutor in Airflow?

**Answer:**  
Local runs tasks in local processes; Celery distributes across worker pool via Redis/RabbitMQ; K8s spins up ephemeral pods per task.

---

### 5. What is the Airflow TaskFlow API?

**Answer:**  
Python decorator syntax (`@task`, `@dag`) allowing implicit XCom passing and cleaner, more pythonic DAG definitions.

---

### 6. What are XComs in Airflow and what is their size limit?

**Answer:**  
Cross-communication mechanism for passing small metadata between tasks; limited by metadata DB backend (1 GB in Postgres, 64 KB in MySQL).

---

### 7. Why should XComs NEVER be used to pass large DataFrames or datasets?

**Answer:**  
Serializes data directly into the Airflow metadata database, causing severe database bloat, memory exhaustion, and scheduler crashes.

---

### 8. What is dynamic DAG generation in Airflow?

**Answer:**  
Creating DAG structures programmatically at parse time from external configuration files (JSON, YAML) or database metadata.

---

### 9. What is the purpose of an Airflow Pool?

**Answer:**  
Limits concurrent execution of tasks that share a common constrained external resource (e.g. database connections, API quotas).

---

### 10. What causes an Airflow Zombie Task?

**Answer:**  
A task instance whose corresponding worker process was killed or vanished without reporting state back to the scheduler.

---

### 11. How does Airflow detect and remediate zombie tasks?

**Answer:**  
The scheduler heartbeat polls task states; if a running task has no active heartbeat for `scheduler_zombie_task_threshold`, it marks it failed.

---

### 12. What is an Airflow Sensor and what is `mode='reschedule'` vs `mode='poke'`?

**Answer:**  
`poke` holds the worker slot continuously while polling; `reschedule` releases the worker slot between retries, saving resources.

---

### 13. What is catchup in Airflow and why should it usually be set to `catchup=False`?

**Answer:**  
Catchup executes all un-run historical intervals since `start_date`; disabling it prevents accidental massive backfill storms.

---

### 14. What is an SLA (Service Level Agreement) in Airflow?

**Answer:**  
Time limit by which a task or DAG should complete relative to its execution date, triggering callback alerts on breach.

---

### 15. What is Infrastructure as Code (IaC) with Terraform?

**Answer:**  
Defining and provisioning cloud infrastructure (storage, clusters, IAM roles) declaratively via human-readable HCL configuration files.

---

### 16. What is Terraform state (`terraform.tfstate`)?

**Answer:**  
A JSON map recording the real-world resources managed by Terraform and matching them to your declarative configuration.

---

### 17. Why must Terraform remote state locking be enabled (e.g. DynamoDB / Azure Blob lease)?

**Answer:**  
Prevents concurrent apply executions from corrupting the state file and causing conflicting infrastructure mutations.

---

### 18. What is pre-commit and why is it essential for data repositories?

**Answer:**  
A framework for managing multi-language git hooks that automatically inspect, lint, and format staged files before commit creation.

---

### 19. What does SQLFluff do?

**Answer:**  
An open-source, dialect-aware SQL linter and auto-formatter that enforces code styling standards across data teams.

---

### 20. What is Slim CI in data pipeline deployment?

**Answer:**  
Compiling modified files and executing tests only on altered code and its downstream dependencies, drastically cutting CI duration and cost.

---

### 21. What is GitHub Actions and how do workflow triggers work?

**Answer:**  
CI/CD automation platform triggered by Git events (`push`, `pull_request`, `schedule`, `workflow_dispatch`).

---

### 22. What is OpenID Connect (OIDC) between GitHub Actions and Cloud Providers?

**Answer:**  
Exchanges short-lived JWT tokens for cloud IAM access, eliminating long-lived static secrets and credentials in CI/CD.

---

### 23. What is Docker and why is it used for data workloads?

**Answer:**  
Containerization platform packaging pipeline code, system dependencies, and Python libraries into an immutable, portable image.

---

### 24. What is a multi-stage Docker build?

**Answer:**  
Build pattern using multiple `FROM` instructions to compile dependencies in temporary stages, producing a lean production runtime container.

---

### 25. What is the Kubernetes Operator pattern for data engineering?

**Answer:**  
Extends Kubernetes API with custom controllers to manage complex stateful data applications (Spark on K8s, Airflow, Flink).

---

### 26. What is the Spark-on-K8s Operator?

**Answer:**  
Kubernetes custom controller that manages the lifecycle of Apache Spark driver and executor pods declaratively via YAML manifests.

---

### 27. What is data-diff in automated testing?

**Answer:**  
Tool that compares row-by-row and column-by-column values between two tables (e.g. staging vs prod) to verify zero unintended schema drift.

---

### 28. What is Great Expectations (GX)?

**Answer:**  
Python library for validating, profiling, and documenting data against declarative assertions (Expectations) in automated pipelines.

---

### 29. What is Soda Core / SodaSQL?

**Answer:**  
Lightweight open-source CLI and library that runs automated SQL checks across datasets against declarative YAML check files.

---

### 30. What is data lineage and why is it critical in DataOps?

**Answer:**  
Tracking the full origin, movement, and transformations of data from raw source ingestion down to consumption dashboards.

---

### 31. What is OpenLineage?

**Answer:**  
An open standard for data lineage collection that instruments pipelines (Airflow, Spark, dbt) to emit runtime lineage events.

---

### 32. What is data observability and what are its 5 pillars?

**Answer:**  
System monitoring focusing on Freshness, Volume, Schema, Quality, and Lineage across enterprise data assets.

---

### 33. Difference between unit testing and integration testing in data pipelines?

**Answer:**  
Unit tests evaluate isolated functions with mock data; integration tests execute end-to-end pipelines against actual databases/storage.

---

### 34. What is an ephemeral development environment in DataOps?

**Answer:**  
Short-lived isolated cloud database schema or bucket created dynamically per pull request and destroyed upon merge.

---

### 35. How do zero-copy clones (Snowflake / Databricks) accelerate CI testing?

**Answer:**  
Instantly duplicates multi-terabyte production metadata without copying underlying storage bytes, enabling full-scale testing at near zero cost.

---

### 36. What is Blue-Green deployment for data platforms?

**Answer:**  
Running two identical production environments (Blue and Green); testing on Green, switching traffic to Green, and keeping Blue for instant rollback.

---

### 37. What is a Canary release in data pipelines?

**Answer:**  
Routing a small percentage (e.g. 5%) of production traffic or consumers to the new pipeline version before full rollout.

---

### 38. What is FinOps in cloud data engineering?

**Answer:**  
Cultural and operational practice combining finance, engineering, and business to drive financial accountability in cloud data spend.

---

### 39. What are the primary cost drivers in Databricks / Snowflake / BigQuery?

**Answer:**  
Idle compute clusters, oversized warehouse SKUs, unpartitioned full table scans, unbounded shuffle spills, and uncompressed storage.

---

### 40. How do query comments and tags enforce resource attribution in DataOps?

**Answer:**  
Injecting Git repo, PR number, team name, and pipeline ID into query metadata allows FinOps to map warehouse credit burn to teams.

---

### 41. What is a circuit breaker in data pipelines?

**Answer:**  
Automated quality gate that immediately halts pipeline execution and aborts downstream updates if anomalous data is detected.

---

### 42. What is a Data Contract in platform architecture?

**Answer:**  
A formal agreement between data producers and data consumers defining schema, SLA, semantics, and governance requirements.

---

### 43. What happens when a data producer introduces an unannounced breaking schema change?

**Answer:**  
Downstream ingestion pipelines break, reports display corrupt metrics, and models fail; prevented by CI schema validation on producer repos.

---

### 44. What is semantic versioning (SemVer) applied to data products?

**Answer:**  
MAJOR version bumps for breaking schema changes; MINOR for backward-compatible additions; PATCH for bug fixes or documentation updates.

---

### 45. What is automated rollback in modern data pipelines?

**Answer:**  
Reverting to the previous immutable table snapshot (e.g. `RESTORE TABLE ... TO VERSION AS OF`) upon automated test failure.

---

### 46. What is secret scanning in GitHub?

**Answer:**  
Automated security check that alerts and blocks commits containing API keys, private keys, or database passwords.

---

### 47. What is trunk-based development vs GitFlow in DataOps?

**Answer:**  
Trunk-based uses short-lived feature branches merged frequently to `main` with CI; GitFlow uses complex long-lived release and develop branches.

---

### 48. What is a Disaster Recovery (DR) RPO and RTO for data platforms?

**Answer:**  
RPO (Recovery Point Objective) is max tolerable data loss duration; RTO (Recovery Time Objective) is max tolerable downtime duration.

---

### 49. How does automated dead-letter routing prevent pipeline blocking?

**Answer:**  
Routes corrupted records to an isolated quarantine location while allowing valid records to proceed through the pipeline.

---

### 50. What is the golden rule of automated testing in data engineering?

**Answer:**  
Never test on production data without isolation; always test transformations against deterministic edge-case fixtures in CI.

---
