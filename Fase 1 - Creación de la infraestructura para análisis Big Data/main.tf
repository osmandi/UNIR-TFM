provider "google" {
  project = var.project_id
  region  = var.region
}

# En este bucket se guardan los notebooks
resource "google_storage_bucket" "tf_gcs_bucket" {
  name     = var.bucket_name
  location = "US"
  force_destroy = true
}

resource "google_dataproc_cluster" "bigdata_dataproc2" {
  name   = var.dataproc_name
  region = var.region

  cluster_config {

    staging_bucket = var.bucket_name

    # Accessing to the app
    ## Configuración por http: enable_http_port_access = true; internal_ip_only=false
    endpoint_config {
      enable_http_port_access = false
    }
    gce_cluster_config {
      internal_ip_only = false
    }

    master_config {
      num_instances    = 1
      machine_type     = var.instance_type_master
      disk_config {
        boot_disk_size_gb =var.boot_size_disk_master
      }
    }

    worker_config {
      num_instances    = var.worker_instances_number
      machine_type     = var.instance_type_worker
      disk_config {
        boot_disk_size_gb = var.boot_size_disk_worker
      }
    }

    software_config {
      image_version = "2.2-ubuntu22"
      optional_components = [
        "JUPYTER",
        "ZOOKEEPER"
      ]
    }


    # Delete cluster after 4 hours
    #lifecycle_config {
    #  idle_delete_ttl = "14400s" # 4 hours
    #}
  }
}
