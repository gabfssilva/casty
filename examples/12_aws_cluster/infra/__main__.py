"""Casty AWS Cluster — Pulumi program."""
from __future__ import annotations

import pulumi

from compute import create_compute
from container import create_container
from load_balancer import create_load_balancer
from network import create_network

# Configuration
config = pulumi.Config()
instance_type = config.get("instance_type") or "t3.small"
node_count = config.get_int("node_count") or 10
key_name = config.get("key_name") or ""

# 1. Networking
net = create_network()

# 2. Container image (ECR + Docker build/push)
container = create_container()

# 3. Compute (EC2 instances)
compute = create_compute(
    subnet_a=net["subnet_a"],
    ec2_sg=net["ec2_sg"],
    image_url=container["repo"].repository_url.apply(lambda url: f"{url}:latest"),
    image_id=container["image"].image_name,
    node_count=node_count,
    instance_type=instance_type,
    key_name=key_name,
)

# 4. Load balancer
lb = create_load_balancer(
    vpc=net["vpc"],
    subnet_a=net["subnet_a"],
    subnet_b=net["subnet_b"],
    alb_sg=net["alb_sg"],
    instances=compute["instances"],
)

# Exports
pulumi.export("alb_url", lb["url"])
pulumi.export(
    "instance_ips",
    [inst.private_ip for inst in compute["instances"]],
)
pulumi.export(
    "instance_public_ips",
    [inst.public_ip for inst in compute["instances"]],
)
pulumi.export("ecr_repo_url", container["repo"].repository_url)
