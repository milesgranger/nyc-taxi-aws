#!/usr/bin/env python3

from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_servicediscovery as servicediscovery,
    aws_elasticloadbalancingv2 as elb,
    aws_logs as logs,
    core,
)


class DaskStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.setup_networking()
        self.setup_cluster()
        self.setup_load_balancer()
        self.create_worker_service()
        self.create_scheduler_service()
        self.create_jupyter_service()
        self.setup_allowable_connections()

    def setup_networking(self):
        self.vpc = ec2.Vpc(
            self,
            id="DaskVPC",
            cidr="10.0.0.0/16",
            max_azs=3,
            default_instance_tenancy=ec2.DefaultInstanceTenancy.DEFAULT,
            enable_dns_support=True,
            enable_dns_hostnames=True,
        )

    def setup_cluster(self):
        self.cluster = ecs.Cluster(
            self,
            id="DaskCluster",
            vpc=self.vpc,
            cluster_name="DaskCluster",
            default_cloud_map_namespace=ecs.CloudMapNamespaceOptions(
                name="local",
                type=servicediscovery.NamespaceType.DNS_PRIVATE,
                vpc=self.vpc,
            ),
        )

    def setup_allowable_connections(self):
        self.scheduler_service.connections.allow_from(
            self.worker_service, ec2.Port.all_tcp()
        )
        self.scheduler_service.connections.allow_from(
            self.jupyter_service, ec2.Port.tcp(port=8786)
        )
        self.worker_service.connections.allow_from(
            self.scheduler_service, ec2.Port.all_tcp()
        )

    def setup_load_balancer(self):
        self.elb = elb.ApplicationLoadBalancer(
            self, id="jupyterLoadBalancer", internet_facing=True, vpc=self.vpc
        )

    def create_worker_service(self):
        worker_task_definition = ecs.FargateTaskDefinition(
            self, id="workerTaskDefinition", cpu=4096, memory_limit_mib=16384
        )
        container = worker_task_definition.add_container(
            id="workerContainer",
            image=ecs.ContainerImage.from_registry(
                "docker.io/milesg/tda-daskworker:latest"
            ),
            command=[
                "dask-worker",
                "tcp://scheduler.local:8786",
                "--nanny",
                "--memory-limit",
                "16G",
                "--worker-port",
                "5555",
            ],
            cpu=4096,
            memory_limit_mib=16384,
            essential=True,
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="worker-", log_retention=logs.RetentionDays.ONE_DAY
            ),
        )
        container.add_port_mappings(
            ecs.PortMapping(
                container_port=8786, host_port=8786, protocol=ecs.Protocol.TCP
            ),
            ecs.PortMapping(
                container_port=8787, host_port=8787, protocol=ecs.Protocol.TCP
            ),
            ecs.PortMapping(
                container_port=9000, host_port=9000, protocol=ecs.Protocol.TCP
            ),
            ecs.PortMapping(
                container_port=5555, host_port=5555, protocol=ecs.Protocol.TCP
            ),
        )
        self.worker_service = ecs.FargateService(
            self,
            id="worker",
            cluster=self.cluster,
            desired_count=8,
            service_name="worker",
            task_definition=worker_task_definition,
        )
        self.worker_service.enable_cloud_map(
            dns_record_type=servicediscovery.DnsRecordType.A, name="worker"
        )
        scaling = self.worker_service.auto_scale_task_count(
            min_capacity=8, max_capacity=18
        )
        scaling.scale_on_cpu_utilization(
            id="workerCpuScaling",
            target_utilization_percent=25,
            disable_scale_in=True,
            scale_out_cooldown=core.Duration.seconds(15),
        )
        scaling.scale_on_memory_utilization(
            id="workerMemoryScaling",
            target_utilization_percent=25,
            disable_scale_in=True,
            scale_out_cooldown=core.Duration.seconds(15),
        )

    def create_scheduler_service(self):
        scheduler_task_definition = ecs.FargateTaskDefinition(
            self, id="schedulerTaskDefinition", cpu=2048, memory_limit_mib=4096
        )
        container = scheduler_task_definition.add_container(
            id="schedulerContainer",
            cpu=2048,
            memory_limit_mib=4096,
            essential=True,
            image=ecs.ContainerImage.from_registry(
                "docker.io/milesg/tda-daskworker:latest"
            ),
            command=["dask-scheduler"],
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="scheduler-", log_retention=logs.RetentionDays.ONE_DAY
            ),
        )
        container.add_port_mappings(
            ecs.PortMapping(
                container_port=8787, host_port=8787, protocol=ecs.Protocol.TCP
            ),
            ecs.PortMapping(
                container_port=8786, host_port=8786, protocol=ecs.Protocol.TCP
            ),
            ecs.PortMapping(
                container_port=9000, host_port=9000, protocol=ecs.Protocol.TCP
            ),
        )

        self.scheduler_service = ecs.FargateService(
            self,
            id="scheduler",
            cluster=self.cluster,
            desired_count=1,
            service_name="scheduler",
            task_definition=scheduler_task_definition,
        )
        self.scheduler_service.enable_cloud_map(
            dns_record_type=servicediscovery.DnsRecordType.A, name="scheduler"
        )

        healthcheck = elb.HealthCheck(
            interval=core.Duration.seconds(60),
            path="/status",
            timeout=core.Duration.seconds(40),
            port="8787",
            healthy_http_codes="200-399",
        )

        satg = elb.ApplicationTargetGroup(
            self,
            id="schedulerTargetGroup",
            port=8787,
            vpc=self.vpc,
            protocol=elb.ApplicationProtocol.HTTP,
            targets=[self.scheduler_service],
            health_check=healthcheck,
        )

        listener = self.elb.add_listener(
            "schedulerPublicListener",
            port=8787,
            open=True,
            protocol=elb.ApplicationProtocol.HTTP,
        )
        listener.add_target_groups(id="schedulerTargetgroups", target_groups=[satg])

    def create_jupyter_service(self):
        jupyter_task_definition = ecs.FargateTaskDefinition(
            self, id="jupyterTaskDefinition", cpu=1024, memory_limit_mib=2048
        )
        container = jupyter_task_definition.add_container(
            id="jupyterContainer",
            cpu=1024,
            memory_limit_mib=2048,
            essential=True,
            image=ecs.ContainerImage.from_registry(
                "docker.io/milesg/tda-daskworker:latest"
            ),
            command=[
                "jupyter",
                "notebook",
                "--NotebookApp.token=supersecret",
                "--ip",
                "0.0.0.0",
                "--no-browser",
                "--allow-root",
            ],
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="jupyter-", log_retention=logs.RetentionDays.ONE_DAY
            ),
        )
        container.add_port_mappings(
            ecs.PortMapping(
                container_port=8888, host_port=8888, protocol=ecs.Protocol.TCP
            )
        )

        self.jupyter_service = ecs.FargateService(
            self,
            id="jupyter",
            cluster=self.cluster,
            desired_count=1,
            service_name="jupyter",
            task_definition=jupyter_task_definition,
            health_check_grace_period=core.Duration.seconds(120),
            assign_public_ip=True,
        )
        self.jupyter_service.enable_cloud_map(
            dns_record_type=servicediscovery.DnsRecordType.A, name="jupyter"
        )

        healthcheck = elb.HealthCheck(
            interval=core.Duration.seconds(60),
            path="/",
            timeout=core.Duration.seconds(40),
            port="8888",
            healthy_http_codes="200-399",
        )

        jatg = elb.ApplicationTargetGroup(
            self,
            id="jupyterTargetGroup",
            port=8888,
            vpc=self.vpc,
            protocol=elb.ApplicationProtocol.HTTP,
            targets=[self.jupyter_service],
            health_check=healthcheck,
        )
        listener = self.elb.add_listener("jupyterPublicListener", port=80, open=True)
        listener.add_target_groups(id="jupyterTargetGroups", target_groups=[jatg])


app = core.App()

DaskStack(app, "aws-stack")

app.synth()
