# GitHub repository infrastructure

The Terraform root module that is **actually applied**. Its sibling (`../`)
provisions Snowflake and never has been, because applying it needs a paid
account — this one manages infrastructure the project genuinely depends on and
costs nothing.

## What it manages

| Resource | Why it is here |
|---|---|
| `github_issue_label` × 2 | `daily-run-breach` and `upstream-stall` were created imperatively by `gh label create ... --force \|\| true` on every scheduled run — infrastructure as a job side effect, with the failure swallowed |
| `github_branch_protection` | ADR 011 said protection *should* require `fast-gate`, `unit`, `behavioral-duckdb`. It said *should* because it was never configured: main was unprotected while the README claimed three **required** checks |
| `github_repository_pages` | The setting whose absence made `dbt-docs.yml` fail at "Configure GitHub Pages" — the workflow was fixed earlier, but there was no site to publish to |
| `github_repository` | Imported. Adds topics (a public repo with none is unfindable) and `delete_branch_on_merge` (merged branches were accumulating) |

## Applying

```bash
export GITHUB_TOKEN=$(gh auth token)   # needs repo admin scope
cd terraform/github
terraform init
terraform plan       # review before applying
terraform apply
```

## First-time import

The repository and the `daily-run-breach` label already existed. They were
**imported**, not recreated — bringing existing infrastructure under management
is the realistic case, and declaring a managed resource without matching its
live state is how IaC adoption breaks the thing it was meant to protect:

```bash
terraform import github_repository.this nyc311-data-platform
terraform import github_issue_label.daily_run_breach nyc311-data-platform:daily-run-breach
```

The first plan after importing showed `3 to add, 1 to change, 0 to destroy`,
with 37 repository attributes unchanged — the check that the import matched.

## State

Local and gitignored. Honest for a single-maintainer repo: there is no second
operator to race with, and the state contains resource metadata that should not
sit in a public repo. A team would need a remote backend with locking; that
pattern is already written down in [`../backend.tf`](../backend.tf).

## Deliberate omissions

- **No `required_pull_request_reviews`.** A single maintainer cannot approve
  their own pull request, so requiring reviews would make merging impossible
  rather than safer. Add it the day a second person joins.
- **`enforce_admins = false`.** On a solo repo, enforcing against admins means a
  broken workflow locks out the only person who could fix it.
- **`strict = false`** on required checks. Requiring branches to be current with
  main forces a rebase every time anything merges; with tiers taking about a
  minute, that churn costs more than the staleness risk.
