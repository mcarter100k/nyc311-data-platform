output "repository_url" {
  description = "The managed repository."
  value       = github_repository.this.html_url
}

output "pages_url" {
  description = "Published dbt documentation site, once the docs workflow has run."
  value       = github_repository_pages.docs.html_url
}

output "required_checks" {
  description = "Status checks that must pass before main accepts a merge."
  value       = github_branch_protection.main.required_status_checks[0].contexts
}
