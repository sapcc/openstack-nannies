# Openstack Nannies

This repository contains so called nannies, which take care of keeping nova, cinder and the vcenter clean and healthy. They contain a growing number of jobs, which find and cleanup inconsistencies in the nova, manila and cinder db, sync quota values, spot orphaned resources and so on. They are still in their early stages and some of the more disruptive functionalities are still in some kind of "reporting only" mode so far.

the provided Dockerfiles to build the corresponding containers contain a placeholder for the image they are based on - the placeholders will have to be replaced by a corresponding image. the below list is what we are using:

* __CINDER_NANNY_BASE_IMAGE__ - a cinder-api image built from here https://github.com/sapcc/kolla/tree/master/docker/cinder/cinder-api
* __MANILA_NANNY_BASE_IMAGE__ - a manila-api image built from here https://github.com/sapcc/kolla/tree/master/docker/manila/manila-api
* __NOVA_NANNY_BASE_IMAGE__ - a nova image build from here https://github.com/sapcc/kolla/blob/master/docker/nova/nova-api/Dockerfile.j2 - it is important, that it contains this fix: https://github.com/sapcc/nova/commit/21c1f4ddf3132af4c83c738c7a7d4a840eed4b17
* __VCENTER_NANNY_BASE_IMAGE__ - we use some internal image, which will be published soon, but some image with python support with the openstack-client and pyvmomi python packages added should do
