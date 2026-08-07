resource "aws_iam_user" "pipeline" {
  name = "crypto-pipeline-writer"
}

resource "aws_iam_access_key" "pipeline" {
  user = aws_iam_user.pipeline.name
}

data "aws_iam_policy_document" "lake_access" {
  statement {
    sid    = "ListOwnBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]

    resources = [aws_s3_bucket.lake.arn]
  }

  statement {
    sid    = "ReadWriteObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]

    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
}

resource "aws_iam_user_policy" "pipeline" {
  name   = "crypto-lake-access"
  user   = aws_iam_user.pipeline.name
  policy = data.aws_iam_policy_document.lake_access.json
}