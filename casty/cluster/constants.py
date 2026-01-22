"""Constants for cluster actor names.

Actor IDs (for local lookups) follow the pattern: {func_name}/{instance_name}
Exposed names (for network lookups) are simpler identifiers.
Instance names are used when creating actors.
"""

# Actor IDs for local lookups (system.actor(name=...))
REMOTE_ACTOR_ID = "remote"
MEMBERSHIP_ACTOR_ID = "membership"

# Exposed/instance names (for network lookups and actor creation)
REMOTE_NAME = "remote"
MEMBERSHIP_NAME = "membership"
SWIM_NAME = "swim"
GOSSIP_NAME = "gossip"
