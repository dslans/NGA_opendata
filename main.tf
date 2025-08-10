
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = "nga-open"
}

resource "google_project_service" "vertexai" {
  service = "aiplatform.googleapis.com"
}

resource "google_project_service" "bigquery" {
  service = "bigquery.googleapis.com"
}

resource "google_service_account" "nga_curator" {
  account_id   = "nga-curator-sa"
  display_name = "NGA Curator Service Account"
}

resource "google_project_iam_member" "bigquery_data_editor" {
  project = "nga-open"
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.nga_curator.email}"
}

resource "google_project_iam_member" "bigquery_user" {
  project = "nga-open"
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.nga_curator.email}"
}

resource "google_project_iam_member" "aiplatform_user" {
  project = "nga-open"
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.nga_curator.email}"
}

resource "google_service_account_key" "nga_curator_key" {
  service_account_id = google_service_account.nga_curator.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

output "service_account_key_path" {
  value = google_service_account_key.nga_curator_key.private_key
  sensitive = true
}

resource "google_service_account" "cloud_build_sa" {
  account_id   = "cloud-build-sa"
  display_name = "Cloud Build Service Account"
}

resource "google_project_iam_member" "cloud_build_editor_role" {
  project = "nga-open"
  role    = "roles/cloudbuild.builds.editor"
  member  = "serviceAccount:${google_service_account.cloud_build_sa.email}"
}

resource "google_project_iam_member" "artifact_registry_writer_role" {
  project = "nga-open"
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.cloud_build_sa.email}"
}

resource "google_project_iam_member" "run_admin_role" {
  project = "nga-open"
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.cloud_build_sa.email}"
}

resource "google_project_iam_member" "service_account_user_role" {
  project = "nga-open"
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.cloud_build_sa.email}"
}

resource "google_project_iam_member" "cloud_build_logs_writer_role" {
  project = "nga-open"
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.cloud_build_sa.email}"
}

resource "google_project_iam_member" "artifact_registry_repo_admin_role" {
  project = "nga-open"
  role    = "roles/artifactregistry.repoAdmin"
  member  = "serviceAccount:${google_service_account.cloud_build_sa.email}"
}
