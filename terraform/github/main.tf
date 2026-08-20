# ---------------------------------------------------------------------------
# GitHub repository infrastructure — THE ROOT MODULE THAT IS ACTUALLY APPLIED
# ---------------------------------------------------------------------------
# The sibling root module (../) provisions Snowflake and has never been applied,
# because applying it requires a paid account. This one manages infrastructure
# the project genuinely depends on and costs nothing: the operational labels the
# breach automation writes to, the branch protection that makes "required
# checks" true rather than aspirational, and the Pages site that serves dbt docs.
#
# Why a SEPARATE root module rather than more resources in ../:
#   a single root would make `terraform plan` require Snowflake credentials AND
#   a GitHub token simultaneously. Splitting them means this one can be applied
#   by anyone with a token, while the Snowflake module stays a design document.
#
# State: local, gitignored. Honest for a single-maintainer repo — there is no
# second operator to race with. A team would need a remote backend with locking
# (the pattern is already written down in ../backend.tf).
#
# Apply:
#   export GITHUB_TOKEN=$(gh auth token)
#   cd terraform/github && terraform init && terraform plan
#
# Resources that already exist must be imported before the first apply — see
# README.md in this directory. Importing existing infrastructure rather than
# recreating it is the point: this repo was not built by Terraform, it is being
# brought under management.

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
  # Token from the GITHUB_TOKEN environment variable — never in a .tf or
  # .tfvars file, matching the credential handling in ../main.tf.
}

# ---------------------------------------------------------------------------
# Repository settings
# ---------------------------------------------------------------------------
# Imported, not created. Every attribute below mirrors the repository's current
# state so the first plan is a no-op except for the changes this module is
# deliberately making (topics, delete_branch_on_merge, pages). Declaring a
# managed resource without matching its live state is how IaC adoption breaks
# things it was meant to protect.

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

  # Changed from the live state on purpose: merged branches were accumulating
  # (eight stale ones at last count) because nothing cleaned them up.
  delete_branch_on_merge = true

  # Changed from the live state on purpose: a public repository with no topics
  # is invisible to every search that would surface it.
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
    # Guard rail on an imported resource: these would be destructive or
    # irreversible if a future edit got them wrong.
    ignore_changes = [auto_init, template]
  }
}

# ---------------------------------------------------------------------------
# GitHub Pages
# ---------------------------------------------------------------------------
# The setting whose absence kept .github/workflows/dbt-docs.yml failing at
# "Configure GitHub Pages" — the workflow was fixed in an earlier PR, but there
# was no Pages site to publish to, so it failed one step later. build_type
# "workflow" means the docs workflow publishes directly; there is no gh-pages
# branch to keep in sync.

resource "github_repository_pages" "docs" {
  repository = github_repository.this.name
  build_type = "workflow"
}

# ---------------------------------------------------------------------------
# Operational labels
# ---------------------------------------------------------------------------
# These were previously created imperatively, on every scheduled run:
#
#     gh label create daily-run-breach --color B60205 --force || true
#
# That is infrastructure created as a side effect of a job, with the failure
# swallowed. Declaring them means the breach automation can assume they exist,
# and their colour and meaning are reviewable in a diff.

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

# ---------------------------------------------------------------------------
# Branch protection
# ---------------------------------------------------------------------------
# ADR 011 said branch protection "should require exactly fast-gate, unit and
# behavioral-duckdb". It said should, because it was never configured — main was
# unprotected while the README claimed three *required* checks. This resource is
# what makes that claim true.

resource "github_branch_protection" "main" {
  repository_id = github_repository.this.node_id
  pattern       = "main"

  required_status_checks {
    # strict = false deliberately: requiring branches to be up to date with main
    # forces a rebase every time anything else merges. With three tiers taking
    # about a minute, the churn costs more than the staleness risk.
    strict   = false
    contexts = ["fast-gate", "unit", "behavioral-duckdb"]
  }

  # NO required_pull_request_reviews block, deliberately. A single maintainer
  # cannot approve their own pull request, so requiring reviews would make
  # merging impossible rather than safer. Add it the day a second person does.

  # false deliberately: leaves the maintainer an escape hatch if CI itself
  # breaks. Enforcing against admins on a solo repo means a broken workflow
  # locks the only person who could fix it out of main.
  enforce_admins = false

  allows_force_pushes = false
  allows_deletions    = false
}
