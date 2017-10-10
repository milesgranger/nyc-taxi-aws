#!/bin/bash
export DEBIAN_FRONTEND=noninteractive
apt update -y
apt install docker.io screen -y
screen -dmS dask-scheduler-screen docker run --network=host milesg/tda-daskworker:latest dask-scheduler
