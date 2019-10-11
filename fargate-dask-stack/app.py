#!/usr/bin/env python3

from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_servicediscovery as servicediscovery,
    aws_ecs_patterns as ecs_patterns,
    aws_route53 as route53,
    aws_logs as logs,
    core,
)


class DaskStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.setup_networking()
        self.setup_security()

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
        self.create_worker_service()
        self.create_scheduler_service()

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

    def setup_security(self):
        # Security group
        self.securitygroup = ec2.CfnSecurityGroup(
            self,
            id="sg-daskSecurityGrp",
            group_description="Security Group for Dask",
            vpc_id=self.vpc.vpc_id,
        )

        # Ingress to allow all resources associated with this security group to talk
        security_group_free_ingress = ec2.CfnSecurityGroupIngress(
            self,
            id="daskSecurityIngress",
            ip_protocol="tcp",
            description="Allow all resources w/ this security group to talk",
            from_port=0,
            to_port=65535,
            group_id=self.securitygroup.ref,
            source_security_group_id=self.securitygroup.attr_group_id,
        )
        security_group_free_ingress.add_depends_on(self.securitygroup)

        # Ingress to allow web views for Jupyter and Bokeh dashboard
        security_group_web_ingress = ec2.CfnSecurityGroupIngress(
            self,
            id="daskSecurityIngressWeb",
            ip_protocol="tcp",
            description="Allow ingress to Jupyter and Bokeh",
            from_port=8786,
            to_port=8888,
            group_id=self.securitygroup.ref,
            source_security_group_id=self.securitygroup.attr_group_id,
        )
        security_group_web_ingress.add_depends_on(self.securitygroup)

        # All egress
        security_group_free_egress = ec2.CfnSecurityGroupEgress(
            self,
            id="daskSecurityEgress",
            ip_protocol="tcp",
            description="Allow all egress",
            from_port=0,
            to_port=65535,
            group_id=self.securitygroup.ref,
            destination_security_group_id=self.securitygroup.attr_group_id,
        )
        security_group_free_egress.add_depends_on(self.securitygroup)

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
        worker_service = ecs.FargateService(
            self,
            id="worker",
            cluster=self.cluster,
            desired_count=2,
            service_name="worker",
            task_definition=worker_task_definition,
            security_group=self.securitygroup,
        )
        worker_service.enable_cloud_map(
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

        scheduler_service = ecs.FargateService(
            self,
            id="scheduler",
            cluster=self.cluster,
            desired_count=1,
            service_name="scheduler",
            task_definition=scheduler_task_definition,
            security_group=self.securitygroup,
        )
        scheduler_service.enable_cloud_map(
            dns_record_type=servicediscovery.DnsRecordType.A, name="scheduler"
        )


app = core.App()

DaskStack(app, "aws-stack")

app.synth()
