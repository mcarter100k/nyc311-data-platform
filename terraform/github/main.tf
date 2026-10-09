# The applied root module: this repository's settings, issue labels, branch
# protection and Pages site. It is separate from the Snowflake module (../) so
# it can be planned with only a GitHub token. State is local and gitignored;
# with one maintainer there is no second operator to race. README.md covers
# applying and the first-time imports.
#
#   export GITHUB_TOKEN=$(gh auth token)
#   cd terraform/github && terraform init && terraform plan

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    github = {
      source  = "integrations/github"
      version = "~> 6.13"
    }
  }
}

provider "github" {
  owner = var.github_owner
  # Token from the GITHUB_TOKEN environment variable.
}

# Imported, not created: attributes match the live repository so a plan
# changes only what this module means to change.

resource "github_repository" "this" {
  name        = "nyc311-data-platform"
  description = "A medallion data platform over NYC 311 service requests — runs daily against the live API, with service level objectives and a published incident record."
  visibility  = "public"

  has_issues   = true
  has_projects = true
  has_wiki     = true

  allow_merge_commit = true
  allow_squash_merge = true
  allow_auto_merge   = false

  # Merged branches are deleted so they do not pile up.
  delete_branch_on_merge = true

  # Topics make the public repository findable in search.
  topics = [
    "data-engineering",
    "dbt",
    "duckdb",
    "airflow",
    "terraform",
    "medallion-architecture",
    "data-quality",
    "nyc-open-data",
  ]

  lifecycle {
    # Changing these on an existing repository would be destructive.
    ignore_changes = [auto_init, template]
  }
}

# The Pages site dbt-docs.yml publishes to. build_type "workflow" means the
# workflow deploys directly; there is no gh-pages branch.

resource "github_repository_pages" "docs" {
  repository = github_repository.this.name
  build_type = "workflow"
}

# Labels the workflows file issues under. Declared here so the workflows can
# assume they exist rather than creating them as a side effect.

resource "github_issue_label" "daily_run_breach" {
  repository  = github_repository.this.name
  name        = "daily-run-breach"
  color       = "B60205"
  description = "Scheduled daily run failed or missed an SLO"
}

resource "github_issue_label" "upstream_stall" {
  repository  = github_repository.this.name
  name        = "upstream-stall"
  color       = "D93F0B"
  description = "Source feed published abnormally little data — not a pipeline failure"
}

resource "github_issue_label" "history_loss" {
  repository  = github_repository.this.name
  name        = "history-loss"
  color       = "FBCA04"
  description = "A daily run did not build on the previous database (ADR 017)"
}

# Requires the three CI tiers from ADR 011 before a merge to main.

resource "github_branch_protection" "main" {
  repository_id = github_repository.this.node_id
  pattern       = "main"

  required_status_checks {
    # Not strict: requiring an up-to-date branch forces a rebase after every
    # merge, which costs more than the staleness risk with minute-long checks.
    strict   = false
    contexts = ["fast-gate", "unit", "behavioral-duckdb"]
  }

  # No required reviews: a sole maintainer cannot approve their own PR.

  # Admins are exempt so a broken CI cannot lock the only maintainer out.
  enforce_admins = false

  allows_force_pushes = false
  allows_deletions    = false
}
