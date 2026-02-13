"""VPC, subnets, internet gateway, route tables, and security groups."""
from __future__ import annotations

import pulumi
import pulumi_aws as aws


def create_network() -> dict:
    """Create VPC, 2 public subnets (multi-AZ for ALB), and security groups.

    Returns dict with keys: vpc, subnet_a, subnet_b, ec2_sg, alb_sg.
    """
    vpc = aws.ec2.Vpc(
        "casty-vpc",
        cidr_block="10.0.0.0/16",
        enable_dns_support=True,
        enable_dns_hostnames=True,
        tags={"Name": "casty-vpc"},
    )

    igw = aws.ec2.InternetGateway(
        "casty-igw",
        vpc_id=vpc.id,
        tags={"Name": "casty-igw"},
    )

    route_table = aws.ec2.RouteTable(
        "casty-rt",
        vpc_id=vpc.id,
        routes=[
            aws.ec2.RouteTableRouteArgs(
                cidr_block="0.0.0.0/0",
                gateway_id=igw.id,
            ),
        ],
        tags={"Name": "casty-rt"},
    )

    # Subnet A — us-east-1a (EC2 instances live here)
    subnet_a = aws.ec2.Subnet(
        "casty-subnet-a",
        vpc_id=vpc.id,
        cidr_block="10.0.1.0/24",
        availability_zone="us-east-1a",
        map_public_ip_on_launch=True,
        tags={"Name": "casty-subnet-a"},
    )

    # Subnet B — us-east-1b (ALB requires 2 AZs)
    subnet_b = aws.ec2.Subnet(
        "casty-subnet-b",
        vpc_id=vpc.id,
        cidr_block="10.0.2.0/24",
        availability_zone="us-east-1b",
        map_public_ip_on_launch=True,
        tags={"Name": "casty-subnet-b"},
    )

    aws.ec2.RouteTableAssociation(
        "casty-rta-a",
        subnet_id=subnet_a.id,
        route_table_id=route_table.id,
    )
    aws.ec2.RouteTableAssociation(
        "casty-rta-b",
        subnet_id=subnet_b.id,
        route_table_id=route_table.id,
    )

    # Security group for ALB
    alb_sg = aws.ec2.SecurityGroup(
        "casty-alb-sg",
        vpc_id=vpc.id,
        description="ALB security group",
        ingress=[
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=80,
                to_port=80,
                cidr_blocks=["0.0.0.0/0"],
            ),
        ],
        egress=[
            aws.ec2.SecurityGroupEgressArgs(
                protocol="-1",
                from_port=0,
                to_port=0,
                cidr_blocks=["0.0.0.0/0"],
            ),
        ],
        tags={"Name": "casty-alb-sg"},
    )

    # Security group for EC2 instances
    ec2_sg = aws.ec2.SecurityGroup(
        "casty-ec2-sg",
        vpc_id=vpc.id,
        description="EC2 instances security group",
        ingress=[
            # ALB → EC2 on port 8000
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=8000,
                to_port=8000,
                security_groups=[alb_sg.id],
            ),
            # EC2 ↔ EC2 internal traffic (Casty TCP + SSH tunnel reverse ports)
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=0,
                to_port=65535,
                cidr_blocks=["10.0.0.0/16"],
            ),
            # SSH (optional)
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=22,
                to_port=22,
                cidr_blocks=["0.0.0.0/0"],
            ),
        ],
        egress=[
            aws.ec2.SecurityGroupEgressArgs(
                protocol="-1",
                from_port=0,
                to_port=0,
                cidr_blocks=["0.0.0.0/0"],
            ),
        ],
        tags={"Name": "casty-ec2-sg"},
    )

    return {
        "vpc": vpc,
        "subnet_a": subnet_a,
        "subnet_b": subnet_b,
        "ec2_sg": ec2_sg,
        "alb_sg": alb_sg,
    }
