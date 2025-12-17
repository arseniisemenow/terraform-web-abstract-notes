terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "~> 0.95"
    }
  }
}

provider "yandex" {
  token = var.yc_token
  cloud_id = var.cloud_id
  folder_id = var.folder_id
  zone = "ru-central1-a"
}


# Resource names
locals {
  network_name = "${var.prefix}-network"
  subnet_name = "${var.prefix}-subnet"
  sg_name = "${var.prefix}-security-group"
  bucket_name = "${var.prefix}-storage"
  db_name = "${var.prefix}-db"
  queue_name = "${var.prefix}-queue"
  registry_name = "${var.prefix}-registry"
  function_name = "${var.prefix}-function"
  worker_name = "${var.prefix}-worker"
  api_name = "${var.prefix}-api"
  lockbox_secret_name = "${var.prefix}-secrets"
  sa_name = "${var.prefix}-sa"
}

# Service Account
resource "yandex_iam_service_account" "main" {
  folder_id = var.folder_id
  name      = local.sa_name
}

# IAM Policies for Service Account
resource "yandex_resourcemanager_folder_iam_member" "sa_admin" {
  folder_id = var.folder_id
  role      = "admin"
  member    = "serviceAccount:${yandex_iam_service_account.main.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_editor" {
  folder_id = var.folder_id
  role      = "editor"
  member    = "serviceAccount:${yandex_iam_service_account.main.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_speechkit" {
  folder_id = var.folder_id
  role      = "ai.speechkit-stt.user"
  member    = "serviceAccount:${yandex_iam_service_account.main.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_gpt" {
  folder_id = var.folder_id
  role      = "ai.languageModels.user"
  member    = "serviceAccount:${yandex_iam_service_account.main.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_storage_admin" {
  folder_id = var.folder_id
  role      = "storage.admin"
  member    = "serviceAccount:${yandex_iam_service_account.main.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_queue_admin" {
  folder_id = var.folder_id
  role      = "editor"
  member    = "serviceAccount:${yandex_iam_service_account.main.id}"
}

# Use existing network
data "yandex_vpc_network" "main" {
  name = "lecture-svc-network"
}

# Create subnet in existing network
resource "yandex_vpc_subnet" "main" {
  name           = local.subnet_name
  zone           = "ru-central1-a"
  network_id     = data.yandex_vpc_network.main.id
  v4_cidr_blocks = ["10.0.1.0/24"]
}

# Security Group
resource "yandex_vpc_security_group" "main" {
  name       = local.sg_name
  network_id = data.yandex_vpc_network.main.id

  ingress {
    protocol       = "TCP"
    description    = "HTTP"
    port           = 8080
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    protocol       = "TCP"
    description    = "HTTPS"
    port           = 8443
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol       = "ANY"
    description    = "All outbound traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }
}

# Object Storage
resource "yandex_storage_bucket" "main" {
  bucket     = local.bucket_name
  access_key = yandex_iam_service_account_static_access_key.main.access_key
  secret_key = yandex_iam_service_account_static_access_key.main.secret_key
}

resource "yandex_iam_service_account_static_access_key" "main" {
  service_account_id = yandex_iam_service_account.main.id
}

# YDB Database
resource "yandex_ydb_database_serverless" "main" {
  name        = local.db_name
  folder_id   = var.folder_id
  description = "Lecture Notes Database"

  serverless_database {
  }

  depends_on = [yandex_resourcemanager_folder_iam_member.sa_editor]
}

# Message Queue
resource "yandex_message_queue" "main" {
  name                        = local.queue_name
  visibility_timeout_seconds  = 3600
  message_retention_seconds   = 86400
  access_key                  = yandex_iam_service_account_static_access_key.main.access_key
  secret_key                  = yandex_iam_service_account_static_access_key.main.secret_key
}

# Container Registry
resource "yandex_container_registry" "main" {
  name = local.registry_name
  folder_id = var.folder_id
}


# Serverless Container - Worker
resource "yandex_serverless_container" "worker" {
  name               = local.worker_name
  folder_id          = var.folder_id
  service_account_id = yandex_iam_service_account.main.id
  memory             = 2048
  execution_timeout  = "3600s"
  concurrency        = 1

  image {
    url = "cr.yandex/mirror/ubuntu:20.04"
  }

  
  depends_on = [
    yandex_ydb_database_serverless.main,
    yandex_message_queue.main,
    yandex_storage_bucket.main,
    yandex_container_registry.main
  ]
}

# Serverless Container - Web API
resource "yandex_serverless_container" "api" {
  name               = local.api_name
  folder_id          = var.folder_id
  service_account_id = yandex_iam_service_account.main.id
  memory             = 512
  execution_timeout  = "60s"
  concurrency        = 10
  description        = "Flask application for Lecture Notes Generator - GET Form Fixed v2"

  image {
    url = "cr.yandex/crptj2umdqses054hv4i/api:fixed-v2"
  }

  
  depends_on = [
    yandex_ydb_database_serverless.main,
    yandex_message_queue.main,
    yandex_storage_bucket.main,
    yandex_container_registry.main
  ]
}

# API Gateway - temporarily commented out due to spec issues
# resource "yandex_api_gateway" "main" {
#   name        = "${var.prefix}-gateway"
#   description = "Lecture Notes Generator API Gateway"
#   folder_id   = var.folder_id
# }

# Lockbox Secret (optional - for better security)
resource "yandex_lockbox_secret" "main" {
  folder_id = var.folder_id
  name      = local.lockbox_secret_name
}

# Outputs
# output "api_gateway_url" {
#   value = "https://${yandex_api_gateway.main.id}.apigw.yandexcloud.net"
#   description = "URL of the API Gateway"
# }

output "storage_bucket_name" {
  value = local.bucket_name
  description = "Name of the Object Storage bucket"
}

output "database_endpoint" {
  value = yandex_ydb_database_serverless.main.document_api_endpoint
  description = "YDB document API endpoint"
  sensitive = true
}

output "service_account_id" {
  value = yandex_iam_service_account.main.id
  description = "Service Account ID"
}

output "container_registry_id" {
  value = yandex_container_registry.main.id
  description = "Container Registry ID"
}

output "api_container_url" {
  value = yandex_serverless_container.api.url
  description = "API Container URL"
}

output "worker_container_url" {
  value = yandex_serverless_container.worker.url
  description = "Worker Container URL"
}

output "message_queue_url" {
  value = yandex_message_queue.main.id
  description = "Message Queue URL"
  sensitive = true
}