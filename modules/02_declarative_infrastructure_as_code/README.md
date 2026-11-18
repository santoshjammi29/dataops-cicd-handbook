# Module 02: Declarative Infrastructure as Code (Terraform & OpenTofu)

## 1. Remote State Locking & Idempotent Lakehouse Provisioning
Data platform infrastructure must be defined declaratively:
- Remote state storage with DynamoDB/Azure Blob locking prevents concurrent apply collisions.
- Storage containers, IAM role bindings, and networking configurations are version-controlled and peer-reviewed.
