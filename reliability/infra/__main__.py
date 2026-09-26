"""The Kubernetes that `python -m reliability` runs the chaos and the performance runs on.

The stack `local` is a kind cluster on this machine: one control plane and `workers` nodes, each a container. On it go
Chaos Mesh, which cuts and slows the links between the nodes of a run; the namespace of the runs; the headless service
that gives the pod of each slot the name it advertises; and the account the runner creates pods and chaos with.

The stack exports the kubeconfig of the cluster and the name kind knows it by, which the image of a run is loaded
under.
"""

import pulumi
import pulumi_command as command
import pulumi_kubernetes as kubernetes
from pulumi_kubernetes.core.v1 import Namespace, Service, ServiceAccount, ServicePortArgs, ServiceSpecArgs
from pulumi_kubernetes.helm.v3 import Release, RepositoryOptsArgs
from pulumi_kubernetes.meta.v1 import ObjectMetaArgs
from pulumi_kubernetes.rbac.v1 import PolicyRuleArgs, Role, RoleBinding, RoleRefArgs, SubjectArgs

CLUSTER = "casty"
NAMESPACE = "casty"
SERVICE = "casty"
"""The headless service: the pod `slot-3` is `slot-3.casty` in the namespace. `reliability/site.py` names it too."""
PORT = 7400
CHAOS_MESH = "2.8.4"

workers = pulumi.Config().get_int("workers") or 3

nodes = "- role: control-plane\n" + "- role: worker\n" * workers
kind = command.local.Command(
    "kind",
    create=f"kind create cluster --name {CLUSTER} --config -",
    delete=f"kind delete cluster --name {CLUSTER}",
    stdin=f"kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n{nodes}",
    triggers=[workers],
    # kind refuses a second cluster of the same name, so a new one waits for the old one to go.
    opts=pulumi.ResourceOptions(delete_before_replace=True),
)
kubeconfig = command.local.Command(
    "kubeconfig",
    create=f"kind get kubeconfig --name {CLUSTER}",
    triggers=[kind.id],
    opts=pulumi.ResourceOptions(depends_on=[kind]),
).stdout

cluster = pulumi.ResourceOptions(provider=kubernetes.Provider("kind", kubeconfig=kubeconfig))

Release(
    "chaos-mesh",
    chart="chaos-mesh",
    version=CHAOS_MESH,
    repository_opts=RepositoryOptsArgs(repo="https://charts.chaos-mesh.org"),
    namespace="chaos-mesh",
    create_namespace=True,
    values={
        # kind runs its pods on containerd.
        "chaosDaemon": {"runtime": "containerd", "socketPath": "/run/containerd/containerd.sock"},
        "controllerManager": {"replicaCount": 1},
        "dashboard": {"create": False},
    },
    opts=cluster,
)

namespace = Namespace("casty", metadata=ObjectMetaArgs(name=NAMESPACE), opts=cluster).metadata.name

Service(
    "nodes",
    metadata=ObjectMetaArgs(name=SERVICE, namespace=namespace),
    spec=ServiceSpecArgs(
        cluster_ip="None",
        # A node is named before it is ready: it dials its seeds by name while it joins, and they dial it back.
        publish_not_ready_addresses=True,
        selector={"casty/role": "node"},
        ports=[ServicePortArgs(name="casty", port=PORT)],
    ),
    opts=cluster,
)

account = ServiceAccount("runner", metadata=ObjectMetaArgs(name="runner", namespace=namespace), opts=cluster)
role = Role(
    "runner",
    metadata=ObjectMetaArgs(name="runner", namespace=namespace),
    rules=[
        PolicyRuleArgs(api_groups=[""], resources=["pods"], verbs=["get", "list", "watch", "create", "delete"]),
        PolicyRuleArgs(api_groups=[""], resources=["pods/log"], verbs=["get"]),
        PolicyRuleArgs(
            api_groups=["chaos-mesh.org"],
            resources=["networkchaos"],
            verbs=["get", "list", "watch", "create", "delete"],
        ),
    ],
    opts=cluster,
)
RoleBinding(
    "runner",
    metadata=ObjectMetaArgs(name="runner", namespace=namespace),
    role_ref=RoleRefArgs(api_group="rbac.authorization.k8s.io", kind="Role", name=role.metadata.name),
    subjects=[SubjectArgs(kind="ServiceAccount", name=account.metadata.name, namespace=namespace)],
    opts=cluster,
)

pulumi.export("kubeconfig", kubeconfig)
pulumi.export("cluster", CLUSTER)
pulumi.export("namespace", namespace)
