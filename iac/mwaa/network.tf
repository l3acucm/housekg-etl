#######################
# Networking (VPC, Subnets, SG for MWAA)
#######################
resource "aws_vpc" "mwaa_vpc" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "subnet_a" {
  vpc_id                  = aws_vpc.mwaa_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = false
}

resource "aws_subnet" "subnet_b" {
  vpc_id                  = aws_vpc.mwaa_vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-1b"
  map_public_ip_on_launch = false
}

resource "aws_security_group" "mwaa_sg" {
  name        = "mwaa_sg"
  description = "Allow access for MWAA"
  vpc_id      = aws_vpc.mwaa_vpc.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}