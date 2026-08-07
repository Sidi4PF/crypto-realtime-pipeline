output "bucket_name" {
  description = "Name of the provisioned data lake bucket."
  value       = aws_s3_bucket.lake.id
}

output "region" {
  value = var.region
}

output "access_key_id" {
  description = "Access key for the pipeline user."
  value       = aws_iam_access_key.pipeline.id
}

output "secret_access_key" {
  description = "Secret key for the pipeline user."
  value       = aws_iam_access_key.pipeline.secret
  sensitive   = true
}