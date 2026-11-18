# Module 03: Automated Regression Testing & Value-Level Data-Diff


![Data Quality Observability & Write-Audit-Publish (WAP) Circuit Breaker Flow](../../assets/whiteboards/data_quality_wap_quarantine_architecture.webp)
> *Architectural Blueprint: [Data Quality Observability & Write-Audit-Publish (WAP) Circuit Breaker Flow](../../docs/whiteboard_diagrams/README.md#data-quality-wap-quarantine). View full multi-persona breakdown and engine mechanisms in the master index.*



## 1. Automated Regression Gates
Code reviews alone cannot detect subtle statistical drift or value mutations caused by SQL alterations. Automated PR pipelines execute value-level diffing between production tables and PR staging tables:
- Identifies unexpected row additions, deletions, or column-level statistical variance before merging.
