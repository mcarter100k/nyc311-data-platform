# GitHub repository infrastructure

The Terraform root module that is **applied**. It manages this repository's own
settings, which cost nothing. Its sibling (`../`) specifies Snowflake and has
never been applied, because applying it needs a paid account. The two are
separate so this one can be planned with only a GitHub token
([ADR 012](../../docs/adr/012-github-repo-as-code.md)).

## What it manages

| Resource | Why it is here |
|---|---|
| `github_issue_label` × 2 | `daily-run-breach` and `upstream-stall`, the labels the workflows file issues under. Declared here so the workflows can assume they exist. |
| `github_branch_protection` | Makes `fast-gate`, `unit` and `behavioral-duckdb` required checks on `main` ([ADR 011](../../docs/adr/011-parallel-ci-tiers.md)). |
| `github_repository_pages` | The Pages site `dbt-docs.yml` deploys the dbt docs to. |
| `github_repository` | Repository settings, plus topics (so the public repo is findable) and `delete_branch_on_merge` (so merged branches do not pile up). |

## Applying

```bash
export GITHUB_TOKEN=$(gh auth token)   # needs repo admin scope
cd terraform/github
terraform init
terraform plan       # review before applying
terraform apply
```

## First-time import

The repository and the `daily-run-breach` label existed before this module, so
they were **imported**, not recreated. Declaring a managed resource without
matching its live state can break the thing it is meant to protect:

```bash
terraform import github_repository.this nyc311-data-platform
terraform import github_issue_label.daily_run_breach nyc311-data-platform:daily-run-breach
```

The first plan after importing showed `3 to add, 1 to change, 0 to destroy`,
with 37 repository attributes unchanged: the check that the import matched.

## State, and what it leaves out

State is local and gitignored. Why, and why there are no required reviews,
`enforce_admins = false` and `strict = false`: see
[ADR 012](../../docs/adr/012-github-repo-as-code.md).
