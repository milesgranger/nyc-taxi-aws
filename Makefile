VERSION := 1.1.1


images:
	docker build . -f docker/DaskWorker-Dockerfile -t milesg/tda-daskworker:$(VERSION) && docker push milesg/tda-daskworker:$(VERSION)

stack:
	cd fargate-dask-stack;\
	cdk synth --path-metadata false --asset-metadata false > stack.yml


.PHONY: images stack