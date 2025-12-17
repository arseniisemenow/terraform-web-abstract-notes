variable "cloud_id" {
  type        = string
  description = "Yandex Cloud ID"
}

variable "folder_id" {
  type        = string
  description = "Yandex Cloud Folder ID"
}

variable "prefix" {
  type        = string
  description = "Prefix for all resources"
  default     = "lecture-notes"
}

variable "yc_token" {
  type        = string
  description = "Yandex Cloud token (from environment)"
  sensitive   = true
}