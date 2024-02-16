###
variable "credentials" {
  description = "My Credentials"
        default     = "./keys/my_creds.json"
}

variable "project" {
  description = "Project"
  default     = "dataengineering-411512"
}

variable "region" {
  description = "Region"
  default     = "europe-central2"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Datset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Bucket Storage Bucket Name"
  default     = "dataengineering-411512-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

