# Module 02: Declarative Infrastructure as Code (Terraform & OpenTofu)

![Databricks Asset Bundles (DABs) & Serverless CI/CD Architecture](../../assets/whiteboards/databricks_dabs_devops_pipeline.webp)
> *Architectural Blueprint: [Databricks Asset Bundles (DABs) & Serverless CI/CD Architecture](../../docs/whiteboard_diagrams/README.md#databricks-dabs-devops-pipeline). View full multi-persona breakdown and engine mechanisms in the master index.*

## 1. Remote State Locking & Idempotent Lakehouse Provisioning
Data platform infrastructure must be defined declaratively:
- Remote state storage with DynamoDB/Azure Blob locking prevents concurrent apply collisions.
- Storage containers, IAM role bindings, and networking configurations are version-controlled and peer-reviewed.
