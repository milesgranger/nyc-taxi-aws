

class ECSCluster:
    """
    Launch ECS Dask cluster
        1 EC2 Instance acting as scheduler
        1 ECS auto-scaling cluster of dask workers
    """
    def __init__(self):
        pass

    def up(self):
        """
        Initialize Dask ECS Auto-scaling cluster
        """

    def _launch_scheduler(self):
        """
        Launch the scheduler
        """
        pass

    def _define_task_definition(self):
        """
        Define ECS task definition
        """
        pass

    def _define_ecs_cluster(self):
        """
        Define ECS cluster settings
        """
        pass

    def _start_cluster(self):
        """
        Launch the cluster
        """
        pass

    def _check_connection(self):
        """
        Verify connection to scheduler and workers have connected.
        """