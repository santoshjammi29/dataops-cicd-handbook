# Module 04: Slim CI & Production State Deferral

![Enterprise Lakehouse CI/CD Multi-Environment Promotion & Rollback Flow](../../assets/whiteboards/lakehouse_cicd_promotion_lifecycle_architecture.webp)
> *Architectural Blueprint: [Enterprise Lakehouse CI/CD Multi-Environment Promotion & Rollback Flow](../../docs/whiteboard_diagrams/README.md#lakehouse-cicd-promotion-lifecycle). View full multi-persona breakdown and engine mechanisms in the master index.*

## 1. Cost-Efficient Pull Request Verification
Running full warehouse rebuilds on every commit wastes tens of thousands of dollars in cloud spend. Slim CI:
- Compares the pull request against the production `manifest.json`.
- Identifies modified nodes and immediate downstreams (`state:modified+`).
- Defers all unmodified upstream tables to production schemas via pointer redirection.
