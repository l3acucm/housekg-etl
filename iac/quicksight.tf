#######################
# QuickSight
resource "aws_quicksight_group" "team" {
  group_name    = "data_team"
  description   = "Data Team Group"
  aws_account_id = data.aws_caller_identity.current.account_id
  namespace     = "default"
}

data "aws_caller_identity" "current" {}