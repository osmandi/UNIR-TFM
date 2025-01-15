variable "project_id" {
  type = string
  description = "ID of the project in which to create resources and add IAM bindings."
}

variable "region" {
  type = string
  description = "The Amazon Web Service region to deploy to"
}

variable bucket_name {
  type = string
  description = "Name of the Google Cloud Storage bucket that will contain the objects."
}

variable dataproc_name {
  type = string
  description = "Name of Dataproc cluster"
}

variable worker_instances_number {
  type = number
  description = "Number of instances to start"
  default = 4
}

variable boot_size_disk_master {
  type = number
  description = "Size of disc in master node"
  default = 50
}

variable boot_size_disk_worker {
  type = number
  description = "Size of disc in worker node"
  default = 100
}

variable "instance_type_master" {
  type = string
  description = "The type of VM instance for master"
  default = "n1-standard-4"
}

variable "instance_type_worker" {
  type = string
  description = "The type of VM instance for workers"
  default = "n2-highmem-4"
}