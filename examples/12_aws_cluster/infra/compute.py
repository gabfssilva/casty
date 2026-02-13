"""EC2 instances with static IPs, IAM role for ECR, and user-data."""
from __future__ import annotations

import json

import pulumi
import pulumi_aws as aws


def create_compute(
    subnet_a: aws.ec2.Subnet,
    ec2_sg: aws.ec2.SecurityGroup,
    image_url: pulumi.Output,
    image_id: pulumi.Output,
    node_count: int,
    instance_type: str,
    key_name: str,
) -> dict:
    """Create IAM role, 10 EC2 instances with static private IPs.

    Returns dict with keys: instances, role.
    """
    # IAM role for EC2 → ECR read access
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
        "casty-ec2-role",
        assume_role_policy=assume_role_policy,
        tags={"Name": "casty-ec2-role"},
    )

    aws.iam.RolePolicyAttachment(
        "casty-ecr-policy",
        role=role.name,
        policy_arn="arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
    )

    instance_profile = aws.iam.InstanceProfile(
        "casty-ec2-profile",
        role=role.name,
    )

    # Get latest Amazon Linux 2023 AMI
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
    instances = []

    for i in range(node_count):
        private_ip = f"10.0.1.{10 + i}"

        user_data = pulumi.Output.all(image_url, image_id).apply(
            lambda args, idx=i, pip=private_ip, sip=seed_ip: _build_user_data(
                args[0], args[1], idx, pip, sip
            )
        )

        instance_args = {
            "ami": ami.id,
            "instance_type": instance_type,
            "subnet_id": subnet_a.id,
            "private_ip": private_ip,
            "vpc_security_group_ids": [ec2_sg.id],
            "iam_instance_profile": instance_profile.name,
            "user_data": user_data,
            "user_data_replace_on_change": True,
            "tags": {"Name": f"casty-node-{i}"},
        }

        if key_name:
            instance_args["key_name"] = key_name

        instance = aws.ec2.Instance(f"casty-node-{i}", **instance_args)
        instances.append(instance)

    return {
        "instances": instances,
        "role": role,
    }


def _build_user_data(
    image_url: str, image_id: str, node_index: int, private_ip: str, seed_ip: str
) -> str:
    """Generate cloud-init user data script."""
    # Extract ECR registry URL (everything before the image name)
    registry = image_url.rsplit("/", 1)[0] if "/" in image_url else image_url

    return f"""#!/bin/bash
# Build: {image_id}
set -ex

# Enable GatewayPorts for SSH reverse tunnels (ClusterClient via SSH)
echo 'GatewayPorts clientspecified' >> /etc/ssh/sshd_config
systemctl restart sshd

# Install Docker
dnf install -y docker
systemctl enable docker
systemctl start docker

# Get region via IMDSv2 (Amazon Linux 2023 default)
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
REGION=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region)

# Login to ECR
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin {registry}

# Pull and run
docker pull {image_url}
docker run -d --restart=always \\
    --network=host \\
    -e NODE_INDEX={node_index} \\
    -e SEED_IPS={seed_ip} \\
    -e CASTY_PORT=25520 \\
    -e HTTP_PORT=8000 \\
    --name casty-node \\
    {image_url}
"""
