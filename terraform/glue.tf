resource "aws_glue_catalog_database" "mlready_db" {
  name        = "mlready"
  description = "Glue database for Gold/ML-Ready Iceberg tabels"
}