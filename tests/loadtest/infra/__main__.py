"""Casty Load Test — Pulumi program."""
from __future__ import annotations

from pathlib import Path

import pulumi
import pulumi_aws as aws

from compute import create_compute
from container import create_container
from load_balancer import create_load_balancer
from network import create_network

config = pulumi.Config()
instance_type = config.get("instance_type") or "t3.small"
node_count = config.get_int("node_count") or 10

# Auto-detect local SSH key and import as EC2 key pair
ssh_key_candidates = ["id_ed25519", "id_rsa", "id_ecdsa"]
ssh_dir = Path.home() / ".ssh"
public_key = None
for name in ssh_key_candidates:
    pub_path = ssh_dir / f"{name}.pub"
    if pub_path.exists():
        public_key = pub_path.read_text().strip()
        break

if not public_key:
    raise RuntimeError(
        f"No SSH public key found in {ssh_dir}. "
        "Generate one with: ssh-keygen -t ed25519"
    )

key_pair = aws.ec2.KeyPair(
    "loadtest-keypair",
    key_name="casty-loadtest",
    public_key=public_key,
    tags={"Name": "casty-loadtest"},
)

# 1. Networking (cluster + load generator security groups)
net = create_network()

# 2. Container image (ECR + Docker build/push)
container = create_container()

# 3. Compute (N cluster nodes + 1 load generator)
compute = create_compute(
    subnet_a=net["subnet_a"],
    cluster_sg=net["cluster_sg"],
    loadgen_sg=net["loadgen_sg"],
    image_url=container["repo"].repository_url.apply(lambda url: f"{url}:latest"),
    image_id=container["image"].image_name,
    node_count=node_count,
    instance_type=instance_type,
    key_name=key_pair.key_name,
)

# 4. Load balancer (optional, for external access)
lb = create_load_balancer(
    vpc=net["vpc"],
    subnet_a=net["subnet_a"],
    subnet_b=net["subnet_b"],
    alb_sg=net["alb_sg"],
    instances=compute["cluster_instances"],
)

# Exports
pulumi.export("alb_url", lb["url"])
pulumi.export(
    "node_ips",
    [inst.private_ip for inst in compute["cluster_instances"]],
)
pulumi.export(
    "node_public_ips",
    [inst.public_ip for inst in compute["cluster_instances"]],
)
pulumi.export("loadgen_public_ip", compute["loadgen_instance"].public_ip)
pulumi.export("ecr_repo_url", container["repo"].repository_url)
