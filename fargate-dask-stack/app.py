#!/usr/bin/env python3

from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_servicediscovery as servicediscovery,
    aws_elasticloadbalancingv2 as elb,
    aws_ecs_patterns as ecs_patterns,
    aws_route53 as route53,
    aws_logs as logs,
    core,
)


class DaskStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.setup_networking()
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
        self.listener = self.elb.add_listener("PublicListener", port=80, open=True)

    def create_worker_service(self):
        worker_task_definition = ecs.FargateTaskDefinition(
            self, id="workerTaskDefinition", cpu=1024, memory_limit_mib=2048
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
                "--worker-port",
                "5555",
            ],
            cpu=1024,
            memory_limit_mib=2048,
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
            desired_count=2,
            service_name="worker",
            task_definition=worker_task_definition,
        )
        self.worker_service.enable_cloud_map(
            dns_record_type=servicediscovery.DnsRecordType.A, name="worker"
        )

    def create_scheduler_service(self):
        scheduler_task_definition = ecs.FargateTaskDefinition(
            self, id="schedulerTaskDefinition", cpu=1024, memory_limit_mib=2048
        )
        container = scheduler_task_definition.add_container(
            id="schedulerContainer",
            cpu=1024,
            memory_limit_mib=2048,
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

        atg = elb.ApplicationTargetGroup(
            self,
            id="appTargetGroup",
            port=8888,
            vpc=self.vpc,
            protocol=elb.ApplicationProtocol.HTTP,
            targets=[self.jupyter_service],
            health_check=healthcheck,
        )
        self.listener.add_target_groups(id="jupyterTargetgroups", target_groups=[atg])


app = core.App()

DaskStack(app, "aws-stack")

app.synth()
