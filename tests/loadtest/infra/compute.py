"""EC2 instances: N cluster nodes + 1 load generator."""
from __future__ import annotations

import json

import pulumi
import pulumi_aws as aws


def create_compute(
    subnet_a: aws.ec2.Subnet,
    cluster_sg: aws.ec2.SecurityGroup,
    loadgen_sg: aws.ec2.SecurityGroup,
    image_url: pulumi.Output,
    image_id: pulumi.Output,
    node_count: int,
    instance_type: str,
    key_name: pulumi.Output,
) -> dict:
    """Create IAM role, N cluster nodes, and 1 load generator.

    Returns dict with keys: cluster_instances, loadgen_instance, role.
    """
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                }
            ],
        }
    )

    role = aws.iam.Role(
        "loadtest-ec2-role",
        assume_role_policy=assume_role_policy,
        tags={"Name": "loadtest-ec2-role"},
    )

    aws.iam.RolePolicyAttachment(
        "loadtest-ecr-policy",
        role=role.name,
        policy_arn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
    )

    instance_profile = aws.iam.InstanceProfile(
        "loadtest-ec2-profile",
        role=role.name,
    )

    ami = aws.ec2.get_ami(
        most_recent=True,
        owners=["amazon"],
        filters=[
            aws.ec2.GetAmiFilterArgs(
                name="name",
                values=["al2023-ami-*-x86_64"],
            ),
            aws.ec2.GetAmiFilterArgs(
                name="state",
                values=["available"],
            ),
        ],
    )

    seed_ip = "10.0.1.10"
    cluster_instances = []

    for i in range(node_count):
        private_ip = f"10.0.1.{10 + i}"

        user_data = pulumi.Output.all(image_url, image_id).apply(
            lambda args, idx=i, pip=private_ip, sip=seed_ip, nc=node_count: _build_node_user_data(
                args[0], args[1], idx, pip, sip, nc
            )
        )

        instance = aws.ec2.Instance(
            f"loadtest-node-{i}",
            ami=ami.id,
            instance_type=instance_type,
            subnet_id=subnet_a.id,
            private_ip=private_ip,
            vpc_security_group_ids=[cluster_sg.id],
            iam_instance_profile=instance_profile.name,
            key_name=key_name,
            user_data=user_data,
            user_data_replace_on_change=True,
            tags={"Name": f"loadtest-node-{i}"},
        )
        cluster_instances.append(instance)

    loadgen_instance = aws.ec2.Instance(
        "loadtest-loadgen",
        ami=ami.id,
        instance_type="t3.medium",
        subnet_id=subnet_a.id,
        private_ip="10.0.1.100",
        vpc_security_group_ids=[loadgen_sg.id],
        key_name=key_name,
        user_data=_build_loadgen_user_data(),
        user_data_replace_on_change=True,
        tags={"Name": "loadtest-loadgen"},
    )

    return {
        "cluster_instances": cluster_instances,
        "loadgen_instance": loadgen_instance,
        "role": role,
    }


def _build_node_user_data(
    image_url: str,
    image_id: str,
    node_index: int,
    private_ip: str,
    seed_ip: str,
    node_count: int,
) -> str:
    registry = image_url.rsplit("/", 1)[0] if "/" in image_url else image_url

    return f"""#!/bin/bash
# Build: {image_id}
set -ex

# Install Docker
dnf install -y docker
systemctl enable docker
systemctl start docker

# Get region via IMDSv2
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
REGION=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region)

# Login to ECR
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin {registry}

# Pull (retry up to 5 times on transient ECR failures)
for attempt in $(seq 1 5); do
    docker pull {image_url} && break
    echo "Pull attempt $attempt failed, retrying in 10s..."
    sleep 10
done

docker run -d --restart=always \\
    --network=host \\
    -e NODE_INDEX={node_index} \\
    -e SEED_IPS={seed_ip} \\
    -e CASTY_PORT=25520 \\
    -e HTTP_PORT=8000 \\
    -e NODE_COUNT={node_count} \\
    --name casty-node \\
    {image_url}
"""


def _build_loadgen_user_data() -> str:
    return """#!/bin/bash
set -ex

# Install Python 3.13
dnf install -y python3.13

# Install uv for ec2-user (user-data runs as root)
sudo -u ec2-user bash -c 'curl -LsSf https://astral.sh/uv/install.sh | sh'
"""
