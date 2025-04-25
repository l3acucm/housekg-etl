#######################
# Glue Catalog Database
#######################
resource "aws_glue_catalog_database" "data_db" {
  name = "realty_data"
}