"""ECR repository and Docker image build/push."""
from __future__ import annotations

import pulumi
import pulumi_aws as aws
import pulumi_docker as docker


def create_container(image_name: str = "casty-loadtest") -> dict:
    """Create ECR repo and build+push the Docker image.

    Returns dict with keys: repo, image.
    """
    repo = aws.ecr.Repository(
        "loadtest-ecr",
        name=image_name,
        force_delete=True,
        image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
            scan_on_push=False,
        ),
        tags={"Name": "loadtest-ecr"},
    )

    token = aws.ecr.get_authorization_token_output(registry_id=repo.registry_id)

    image = docker.Image(
        "loadtest-image",
        build=docker.DockerBuildArgs(
            context="../../..",  # casty root (relative to infra/ CWD)
            dockerfile="../Dockerfile",  # relative to infra/ CWD
            platform="linux/amd64",
        ),
        image_name=repo.repository_url.apply(lambda url: f"{url}:latest"),
        registry=docker.RegistryArgs(
            server=repo.repository_url,
            username=token.user_name,
            password=pulumi.Output.secret(token.password),
        ),
    )

    return {
        "repo": repo,
        "image": image,
    }
