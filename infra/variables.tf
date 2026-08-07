variable "region" {
  description = "AWS region hosting the data lake."
  type        = string
  default     = "eu-west-3"
}

variable "bucket_prefix" {
  description = "Prefix for the globally unique bucket name."
  type        = string
  default     = "crypto-lake"
}

variable "budget_limit_usd" {
  description = "Monthly spend threshold that triggers an email alert."
  type        = string
  default     = "1"
}

variable "alert_email" {
  description = "Address receiving budget alerts."
  type        = string
}

variable "raw_data_expiration_days" {
  description = "Days before bronze objects are deleted."
  type        = number
  default     = 7
}