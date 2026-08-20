output "repository_url" {
  description = "The managed repository."
  value       = github_repository.this.html_url
}

output "pages_url" {
  description = "Published dbt documentation site, once the docs workflow has run."
  value       = try(github_repository.this.pages[0].html_url, null)
}

output "required_checks" {
  description = "Status checks that must pass before main accepts a merge."
  value       = github_branch_protection.main.required_status_checks[0].contexts
}
