

.PHONY: all

dask-ecs:
	docker build . -f docker/DaskWorker-Dockerfile -t milesg/tda-daskworker:latest && docker push milesg/tda-daskworker:latest
