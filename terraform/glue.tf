resource "aws_glue_catalog_databasae" "mlready_db" {
    name = "mlready"
    description = "Glue database for Gold/ML-Ready Iceberg tabels"
}