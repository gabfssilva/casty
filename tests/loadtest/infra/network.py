"""VPC, subnets, internet gateway, route tables, and security groups."""
from __future__ import annotations

import pulumi_aws as aws


def create_network() -> dict:
    """Create VPC, 2 public subnets, and security groups for cluster + load generator.

    Returns dict with keys: vpc, subnet_a, subnet_b, cluster_sg, loadgen_sg, alb_sg.
    """
    vpc = aws.ec2.Vpc(
        "loadtest-vpc",
        cidr_block="10.0.0.0/16",
        enable_dns_support=True,
        enable_dns_hostnames=True,
        tags={"Name": "loadtest-vpc"},
    )

    igw = aws.ec2.InternetGateway(
        "loadtest-igw",
        vpc_id=vpc.id,
        tags={"Name": "loadtest-igw"},
    )

    route_table = aws.ec2.RouteTable(
        "loadtest-rt",
        vpc_id=vpc.id,
        routes=[
            aws.ec2.RouteTableRouteArgs(
                cidr_block="0.0.0.0/0",
                gateway_id=igw.id,
            ),
        ],
        tags={"Name": "loadtest-rt"},
    )

    # Subnet A — us-east-1a (EC2 instances + load generator)
    subnet_a = aws.ec2.Subnet(
        "loadtest-subnet-a",
        vpc_id=vpc.id,
        cidr_block="10.0.1.0/24",
        availability_zone="us-east-1a",
        map_public_ip_on_launch=True,
        tags={"Name": "loadtest-subnet-a"},
    )

    # Subnet B — us-east-1b (ALB requires 2 AZs)
    subnet_b = aws.ec2.Subnet(
        "loadtest-subnet-b",
        vpc_id=vpc.id,
        cidr_block="10.0.2.0/24",
        availability_zone="us-east-1b",
        map_public_ip_on_launch=True,
        tags={"Name": "loadtest-subnet-b"},
    )

    aws.ec2.RouteTableAssociation(
        "loadtest-rta-a",
        subnet_id=subnet_a.id,
        route_table_id=route_table.id,
    )
    aws.ec2.RouteTableAssociation(
        "loadtest-rta-b",
        subnet_id=subnet_b.id,
        route_table_id=route_table.id,
    )

    # Security group for ALB
    alb_sg = aws.ec2.SecurityGroup(
        "loadtest-alb-sg",
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
        tags={"Name": "loadtest-alb-sg"},
    )

    # Security group for cluster nodes
    cluster_sg = aws.ec2.SecurityGroup(
        "loadtest-cluster-sg",
        vpc_id=vpc.id,
        description="Cluster nodes security group",
        ingress=[
            # ALB -> EC2 on port 8000
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=8000,
                to_port=8000,
                security_groups=[alb_sg.id],
            ),
            # Load generator -> EC2 on port 8000
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=8000,
                to_port=8000,
                cidr_blocks=["10.0.1.100/32"],
            ),
            # EC2 <-> EC2 on Casty TCP port
            aws.ec2.SecurityGroupIngressArgs(
                protocol="tcp",
                from_port=25520,
                to_port=25520,
                cidr_blocks=["10.0.0.0/16"],
            ),
            # SSH
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
        tags={"Name": "loadtest-cluster-sg"},
    )

    # Security group for load generator
    loadgen_sg = aws.ec2.SecurityGroup(
        "loadtest-loadgen-sg",
        vpc_id=vpc.id,
        description="Load generator security group",
        ingress=[
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
        tags={"Name": "loadtest-loadgen-sg"},
    )

    return {
        "vpc": vpc,
        "subnet_a": subnet_a,
        "subnet_b": subnet_b,
        "cluster_sg": cluster_sg,
        "loadgen_sg": loadgen_sg,
        "alb_sg": alb_sg,
    }
