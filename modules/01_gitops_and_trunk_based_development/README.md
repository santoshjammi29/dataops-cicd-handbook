# Module 01: GitOps & Trunk-Based Development for Data Platforms

## 1. Trunk-Based Development Paradigm
Long-lived feature branches lead to catastrophic merge conflicts in shared data schemas. In trunk-based development:
- Developers branch from `main`, make focused changes, and merge via pull request within 24-48 hours.
- Ephemeral schemas (`pr_1234_staging`) are spun up dynamically for automated testing and destroyed on merge.
