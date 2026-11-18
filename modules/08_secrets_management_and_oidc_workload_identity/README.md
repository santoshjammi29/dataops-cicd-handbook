# Module 08: Keyless Cloud Authentication via OIDC Workload Identity

## 1. Eliminating Static Credentials
Long-lived API tokens and AWS/Azure access keys represent severe security liabilities. With OpenID Connect (OIDC):
- CI/CD runners (GitHub Actions) receive a signed JSON Web Token (JWT) from GitHub.
- Cloud providers (AWS STS, Azure Managed Identity, GCP Workload Identity) validate the token signature and issue temporary, short-lived (15-min) credentials.
