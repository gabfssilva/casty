"""ECR repository and Docker image build/push."""
from __future__ import annotations

import pulumi
import pulumi_aws as aws
import pulumi_docker as docker


def create_container(image_name: str = "casty-aws-cluster") -> dict:
    """Create ECR repo and build+push the Docker image.

    Returns dict with keys: repo, image.
    """
    repo = aws.ecr.Repository(
        "casty-ecr",
        name=image_name,
        force_delete=True,
        image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
            scan_on_push=False,
        ),
        tags={"Name": "casty-ecr"},
    )

    # Get ECR credentials for docker push
    token = aws.ecr.get_authorization_token_output(registry_id=repo.registry_id)

    image = docker.Image(
        "casty-image",
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
