variable "credentials" {
  description = "Path to my GCP Credentials"
  default     = "./google_credentials.json"
}

variable "project" {
  description = "Your GCP Project ID"
  default     = "dez-wiki-monitor" 
}

variable "region" {
  description = "Region for resources"
  default     = "europe-west1"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "wiki_edits_data"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  # Bucket names must be globally unique!
  default     = "dez-wiki-lake-unique-id" 
}