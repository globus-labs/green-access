from globus_compute_endpoint.endpoint.utils.config import Config
from globus_compute_endpoint.engines import GlobusComputeEngine
from parsl.providers import LocalProvider
from parsl.monitoring import MonitoringHub, MessageRoutingOptions, MessageType

config = Config(
    display_name=None,  # If None, defaults to the endpoint name
    executors=[
        GlobusComputeEngine(
            max_workers=16, # TODO: Update with number of cores on machine
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
                worker_init="conda activate funcx-dev"
            ),
            strategy=None,
            energy_monitor="RaplCPUNodeEnergyMonitor",
            cpu_affinity="block",
        )
    ],
    monitoring_hub=MonitoringHub(
        hub_address="localhost",
        hub_port=55055,
        monitoring_debug=True,
        resource_monitoring_interval=1,
        routing_policy=MessageRoutingOptions.KAFKA,
        kafka_config={
            # TODO: Replace these topics!!!
            MessageType.WORKFLOW_INFO: "green-faas-resources",
            MessageType.TASK_INFO: "green-faas-prediction",
            MessageType.RESOURCE_INFO: "green-faas-resources",
            MessageType.ENERGY_INFO: "green-faas-resources",
        }
    ),
)

# For now, visible_to must be a list of URNs for globus auth users or groups, e.g.:
# urn:globus:auth:identity:{user_uuid}
# urn:globus:groups:id:{group_uuid}
meta = {
    "name": "peony-funcx-dev",
    "description": "",
    "organization": "",
    "department": "",
    "public": False,
    "visible_to": [],
}
