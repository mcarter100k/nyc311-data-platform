# GitHub repository infrastructure

Terraform code for this repository's own GitHub settings. Terraform describes
infrastructure in files, and `terraform apply` makes the live settings match
them. This is the one Terraform module in the repo that is **applied**; the
settings it manages cost nothing. Its sibling (`../`) specifies Snowflake and has
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
they were **imported**, not recreated. Without an import, Terraform would try
to create a second copy or overwrite the live settings:

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
