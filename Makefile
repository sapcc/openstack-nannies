IMAGE_REPO:=hub.global.cloud.sap/monsoon
IMAGE_TAG:=$(shell date -u +%Y%m%d%H%M%S)

all: manila-nanny netapp-nanny

manila-nanny: manila-stein
netapp-nanny: netapp-manila-stein

manila-stein: Dockerfile.manila-nanny docker-stein
	docker build --network=host --no-cache \
	    -t ${IMAGE_REPO}/manila-nanny:stein-${IMAGE_TAG} \
	    --build-arg __MANILA_NANNY_BASE_IMAGE__=hub.global.cloud.sap/monsoon/loci-manila:stein-latest \
	    -f Dockerfile.manila-nanny .
	docker tag ${IMAGE_REPO}/manila-nanny:stein-${IMAGE_TAG} ${IMAGE_REPO}/manila-nanny:stein-latest
	docker push ${IMAGE_REPO}/manila-nanny:stein-${IMAGE_TAG}
	docker push ${IMAGE_REPO}/manila-nanny:stein-latest

netapp-manila-stein: Dockerfile.netapp-manila-nanny docker-stein
	docker build -t ${IMAGE_REPO}/netapp-manila-nanny:stein-${IMAGE_TAG} \
	    --build-arg __MANILA_NANNY_BASE_IMAGE__=hub.global.cloud.sap/monsoon/loci-manila:stein-latest \
	    -f Dockerfile.netapp-manila-nanny .
	docker tag ${IMAGE_REPO}/netapp-manila-nanny:stein-${IMAGE_TAG} ${IMAGE_REPO}/netapp-manila-nanny:stein-latest
	docker push ${IMAGE_REPO}/netapp-manila-nanny:stein-${IMAGE_TAG}
	docker push ${IMAGE_REPO}/netapp-manila-nanny:stein-latest

manila-train: Dockerfile.manila-nanny docker-train
	docker build --network=host --no-cache \
		  -t ${IMAGE_REPO}/manila-nanny:train-${IMAGE_TAG} \
	    --build-arg __MANILA_NANNY_BASE_IMAGE__=hub.global.cloud.sap/monsoon/loci-manila:train-latest \
	    -f Dockerfile.manila-nanny .
	docker tag ${IMAGE_REPO}/manila-nanny:train-${IMAGE_TAG} ${IMAGE_REPO}/manila-nanny:train-latest
	docker push ${IMAGE_REPO}/manila-nanny:train-${IMAGE_TAG}
	docker push ${IMAGE_REPO}/manila-nanny:train-latest

netapp-manila-train: Dockerfile.netapp-manila-nanny docker-train
	docker build -t ${IMAGE_REPO}/netapp-manila-nanny:train-${IMAGE_TAG} \
	    --build-arg __MANILA_NANNY_BASE_IMAGE__=hub.global.cloud.sap/monsoon/loci-manila:train-latest \
	    -f Dockerfile.netapp-manila-nanny .
	docker tag ${IMAGE_REPO}/netapp-manila-nanny:train-${IMAGE_TAG} ${IMAGE_REPO}/netapp-manila-nanny:train-latest
	docker push ${IMAGE_REPO}/netapp-manila-nanny:train-${IMAGE_TAG}
	docker push ${IMAGE_REPO}/netapp-manila-nanny:train-latest

docker-train:
	docker pull hub.global.cloud.sap/monsoon/loci-manila:train-latest

docker-stein:
	docker pull hub.global.cloud.sap/monsoon/loci-manila:stein-latest


.Phony: manila netapp manila-stein netapp-manila-stein
