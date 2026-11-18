# Module 07: Kubernetes Operators for Distributed Data (Spark on K8s)

## 1. Spark on Kubernetes Operator
The Kubernetes Operator eliminates dedicated standalone Spark clusters:
- Driver and Executor pods are scheduled dynamically on demand.
- Spot node tolerance with graceful decommissioning prevents job failure during node reclamation.
