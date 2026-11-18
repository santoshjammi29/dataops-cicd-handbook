# Module 04: Slim CI & Production State Deferral

## 1. Cost-Efficient Pull Request Verification
Running full warehouse rebuilds on every commit wastes tens of thousands of dollars in cloud spend. Slim CI:
- Compares the pull request against the production `manifest.json`.
- Identifies modified nodes and immediate downstreams (`state:modified+`).
- Defers all unmodified upstream tables to production schemas via pointer redirection.
