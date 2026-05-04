resource "aws_ecr_repository" "recommender_embeddings" {
  name                 = "recommender-embeddings"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Retention policy - I only keep the last 3 images
resource "aws_ecr_lifecycle_policy" "recommender_embeddings_policy" {
  repository = aws_ecr_repository.recommender_embeddings.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep only the last 3 images"
      selection = {
        tagStatus     = "any"
        countType     = "imageCountMoreThan"
        countNumber   = 3
      }
      action = {
        type = "expire"
      }
    }]
  })
}