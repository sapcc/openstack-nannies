import functools

from netapp_ontap.error import NetAppRestError
from netapp_ontap.host_connection import HostConnection
from netapp_ontap.resources import (CLI, Aggregate, Cluster, ClusterPeer,
                                    Snapshot, Svm, Volume)


def with_connection(func):

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if 'connection' not in kwargs:
            kwargs['connection'] = self.connection
        return func(self, *args, **kwargs)

    return wrapper


"""
docu for netapp_ontap: https://library.netapp.com/ecmdocs/ECMLP2858435/html/

The get_collection method is lazy and makes a call only when being iterated.
To fail early we always make a list in such cases
e.g. `list(Aggregate.get_collection(**kwargs))`
"""


class NetAppRestHelper:

    def __init__(self, host, user, password, verify_ssl=False, debug=False):
        self.host = host
        self.user = user
        self.password = password
        self.verify_ssl = verify_ssl

        if debug:
            from netapp_ontap import utils
            utils.DEBUG = 1
            utils.LOG_ALL_API_CALLS = 1

        self.connection = self.create_connection()
        self.test_connection()

    def create_connection(self):
        return HostConnection(self.host, self.user, self.password, verify=self.verify_ssl)

    def test_connection(self):
        try:
            self.get_cluster()
        except NetAppRestError as e:
            if e.status_code == 404:
                raise Exception('NetApp RestAPI not supported')
            raise

    @with_connection
    def get_aggregates(self, **kwargs):
        return list(Aggregate.get_collection(**kwargs))

    def space_info(self):
        space_info = {}
        for aggr in self.get_aggregates(fields='space'):
            space_info[aggr.name] = aggr.space.block_storage

        return space_info

    def get_cluster(self, **kwargs):
        cluster = Cluster()
        cluster.set_connection(self.connection)
        cluster.get(**kwargs)
        return cluster

    @with_connection
    def get_cluster_peers(self, **kwargs):
        try:
            cluster_peers = list(ClusterPeer.get_collection(**kwargs))
            # load each item to get all attributes
            for peer in cluster_peers:
                peer.get()
        except NetAppRestError as e:
            if e.status_code == 404:
                raise Exception('NetApp ClusterPeer RestAPI not supported on version {}'.format(
                    self.version))
            raise
        return cluster_peers

    @property
    def version(self):
        return self.get_cluster().version.full

    @with_connection
    def get_vservers(self, **kwargs):
        return list(Svm.get_collection(**kwargs))

    @with_connection
    def get_volumes(self, **kwargs):
        return list(Volume.get_collection(**kwargs))

    def get_volumes_in_vserver(self, vserver):
        return self.get_volumes(**{"svm.name": vserver})

    @with_connection
    def get_snapshots(self, volume_uuid, **kwargs):
        return list(Snapshot.get_collection(volume_uuid, **kwargs))

    def execute_cli(self, cmd, **kwargs):
        """
        Call any ONTAP CLI (the one you get on ssh login)
        e.g. 'aggr show -root false -fields percent-used,availsize' via
        execute_cli('aggr show', root=False, fields='percent-used,availsize')
        """
        cli = CLI()
        cli.set_connection(self.connection)
        response = cli.execute(cmd, **kwargs)
        return response.http_response.json()
