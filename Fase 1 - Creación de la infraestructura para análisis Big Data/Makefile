# Install GCP provider
install:
	terraform init

# Create and apply changes
apply:
	terraform apply -var-file=development.tfvars

# Show changes
plan:
	terraform plan -var-file=development.tfvars

# Restore the status of terraform
restore:
	terraform state pull -var-file=development.tfvars > terraform.tfstate

# Destroy everything without confirmation
destroy:
	terraform destroy -var-file=development.tfvars -auto-approve