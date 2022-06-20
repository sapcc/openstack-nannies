# grab latest nanny image (no immediate expansion; := vs =)
# https://stackoverflow.com/a/10081105
NANNY_IMAGE=$(shell kubectl get pod -n monsoon3 -l "component=manila-nanny" -o jsonpath='{.items[].spec.containers[0].image}')

.PHONY: Dockerfile.manila-nanny-debug
Dockerfile.manila-nanny-debug: Dockerfile.manila-nanny
	@sed s,__MANILA_NANNY_BASE_IMAGE__,$(NANNY_IMAGE),g Dockerfile.manila-nanny > Dockerfile.manila-nanny-debug
