# Module 08: Keyless Cloud Authentication via OIDC Workload Identity

![Lakehouse Fine-Grained Access Control, Dynamic Masking & Entra/ABAC Enforcement](../../assets/whiteboards/lakehouse_access_masking_lifecycle_architecture.webp)
> *Architectural Blueprint: [Lakehouse Fine-Grained Access Control, Dynamic Masking & Entra/ABAC Enforcement](../../docs/whiteboard_diagrams/README.md#lakehouse-access-masking-lifecycle). View full multi-persona breakdown and engine mechanisms in the master index.*

## 1. Eliminating Static Credentials
Long-lived API tokens and AWS/Azure access keys represent severe security liabilities. With OpenID Connect (OIDC):
- CI/CD runners (GitHub Actions) receive a signed JSON Web Token (JWT) from GitHub.
- Cloud providers (AWS STS, Azure Managed Identity, GCP Workload Identity) validate the token signature and issue temporary, short-lived (15-min) credentials.
