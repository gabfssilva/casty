"""Application Load Balancer, target group, and listener."""

from __future__ import annotations

import pulumi_aws as aws


def create_load_balancer(
    vpc: aws.ec2.Vpc,
    subnet_a: aws.ec2.Subnet,
    subnet_b: aws.ec2.Subnet,
    alb_sg: aws.ec2.SecurityGroup,
    instances: list[aws.ec2.Instance],
) -> dict:
    """Create ALB with health-checked target group.

    Returns dict with keys: alb, target_group, listener, url.
    """
    alb = aws.lb.LoadBalancer(
        "loadtest-alb",
        internal=False,
        load_balancer_type="application",
        security_groups=[alb_sg.id],
        subnets=[subnet_a.id, subnet_b.id],
        tags={"Name": "loadtest-alb"},
    )

    target_group = aws.lb.TargetGroup(
        "loadtest-tg",
        port=8000,
        protocol="HTTP",
        vpc_id=vpc.id,
        target_type="instance",
        health_check=aws.lb.TargetGroupHealthCheckArgs(
            path="/health",
            port="8000",
            protocol="HTTP",
            healthy_threshold=2,
            unhealthy_threshold=3,
            interval=15,
            timeout=5,
        ),
        tags={"Name": "loadtest-tg"},
    )

    for i, instance in enumerate(instances):
        aws.lb.TargetGroupAttachment(
            f"loadtest-tga-{i}",
            target_group_arn=target_group.arn,
            target_id=instance.id,
            port=8000,
        )

    listener = aws.lb.Listener(
        "loadtest-listener",
        load_balancer_arn=alb.arn,
        port=80,
        protocol="HTTP",
        default_actions=[
            aws.lb.ListenerDefaultActionArgs(
                type="forward",
                target_group_arn=target_group.arn,
            ),
        ],
    )

    url = alb.dns_name.apply(lambda dns: f"http://{dns}")

    return {
        "alb": alb,
        "target_group": target_group,
        "listener": listener,
        "url": url,
    }
