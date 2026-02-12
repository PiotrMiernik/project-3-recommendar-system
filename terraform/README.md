# Project 3 – Recommender System

This directory contains Infrastructure as Code (IaC) definitions for the AWS resources used in Project 3.

---

## Architecture (Stage 1)

The following resources are provisioned:

### S3

- 1 data lake bucket
- Logical prefixes: `raw/`, `silver/`, `gold/`
- Encryption enabled (SSE-S3)
- Versioning enabled
- Public access blocked

### IAM

- `orchestrator_role` – assumed by local Airflow via STS AssumeRole
- `emr_service_role` – used by EMR control plane
- `emr_ec2_role` + instance profile – used by EMR cluster EC2 instances
- Least privilege S3 access scoped to specific prefixes

### RDS (PostgreSQL 15)

- Used for metadata and pgvector embeddings
- Encrypted storage
- Publicly accessible (restricted by security group)
- Inbound access limited via `allowed_cidr_blocks`

---

## Configuration

Terraform variables are defined in:

`variables.tf`

### Required Local File (Not Committed)

Create a local file:

`terraform.tfvars`

Use the template:

`terraform.tfvars.example`

The real `terraform.tfvars` file must:

- Contain real values
- Be added to `.gitignore`
- Never contain placeholder secrets
- Never be committed to the repository

---

## Security Notes

- `db_password` is marked as sensitive.
- RDS access is restricted to the CIDRs defined in `allowed_cidr_blocks`.
- For development, use your public IP with `/32` mask.
- No secrets are stored in the repository.
- Runtime application credentials are stored separately in `config/.env`.

---

## Deployment

Run the following commands from inside the `terraform/` directory:

```bash
terraform init
terraform validate
terraform plan
terraform apply
```

## To destroy infrastructure

terraform destroy

## Outputs

After `terraform apply`, Terraform returns:

* S3 bucket name
* RDS endpoint and port
* Orchestrator role ARN
* EMR roles and instance profile

Use these values to populate `config/.env` for local Airflow.

## Cost Model

This setup is optimized for minimal cost:

* Small RDS instance (`db.t4g.micro`)
* EMR cluster is not persistent (job cluster model)
* Airflow runs locally
* Infrastructure should be destroyed when not needed to avoid unnecessary charges
