resource "google_cloudbuild_trigger" "main_branch_merge_trigger" {
  project     = "nga-open"
  description = "Trigger for merges to the main branch"

  github {
    owner = "dslans"
    name  = "NGA_opendata"
    pull_request {
      branch          = "^main$"
      comment_control = "COMMENTS_DISABLED"
      
    }
  }

  filename      = "cloudbuild.yaml"
  service_account = "projects/nga-open/serviceAccounts/983922642462@cloudbuild.gserviceaccount.com"
}