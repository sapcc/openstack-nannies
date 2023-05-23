from __future__ import absolute_import

import argparse
import configparser
import copy
import datetime
import http.server
import json
import logging
import sys
import time
from threading import Thread

from keystoneauth1 import session
from keystoneauth1.identity import v3
from manilaclient import client
from prometheus_client import start_http_server
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

log = logging.getLogger(__name__)


class ManilaNanny(http.server.HTTPServer):
    ''' Manila Nanny '''
    def __init__(self, config_file, interval, dry_run=False, prom_port=0, address="", http_port=8000, handler=None, version="2.65", **extra_args):
        self.config_file = config_file
        self.interval = interval
        self.dry_run = dry_run
        self.microversion = version
        self.init_db_connection()
        self.manilaclient = create_manila_client(config_file, self.microversion)

        if prom_port != 0:
            try:
                start_http_server(prom_port)
            except Exception as e:
                sys.stdout.write("prometheus_client:start_http_server: " + str(e) + "\n")
                sys.exit(-1)

        # Initialize the class as http server. The handler needs to access the variables
        # in the class
        if handler:
            super(ManilaNanny, self).__init__((address, http_port), handler)
            thread = Thread(target=self.serve_forever, args=())
            thread.setDaemon(True)
            thread.start()

        if 'pdb_port' in extra_args and extra_args['pdb_port']:
            import pdb_attach
            pdb_attach.listen(extra_args['pdb_port'])

    def _run(self):
        raise Exception('not implemented')

    def run(self):
        while True:
            self._run()
            # allow pdb_attach to work while waiting for the next run
            wait_interval = self.interval
            while wait_interval > 0:
                wait_interval -= 1
                time.sleep(1)

    def init_db_connection(self):
        """Establish a database connection"""
        db_url = self.get_db_url()
        engine = create_engine(db_url, pool_recycle=3600)
        engine.connect()
        Session = sessionmaker(bind=engine)
        self.db_session = Session()
        self.db_metadata = MetaData()
        self.db_metadata.bind = engine
        self.db_base = declarative_base()
        self.engine = engine

    def get_db_url(self):
        """Return the database connection string from the config file"""
        try:
            parser = configparser.ConfigParser()
            parser.read(self.config_file)
            db_url = parser.get('database', 'connection', raw=True)
        except Exception as e:
            print(f'ERROR: Parse {self.config_file}: ' + str(e))
            sys.exit(2)
        return db_url

    def db_table(self, table_name):
        """Return the database table"""
        return Table(table_name, self.db_metadata, autoload=True)

    def renew_manila_client(self):
        self.manilaclient = create_manila_client(self.config_file, self.microversion)

    def undefined_route(self, route):
        status_code = 500
        header = ('Content-Type', 'text/html; charset=UTF-8')
        message_parts = [
            f'{route} is not defined in {self.__class__}',
        ]
        message = '\r\n'.join(message_parts)
        return status_code, header, message

    def share_reset_state(self, share_id, state):
        try:
            self.manilaclient.shares.reset_state(share_id, state)
        except Exception as e:
            log.exception("share_reset_state(share_id=%s, state=%s): %s", share_id, state, e)

    def share_instance_reset_state(self, share_instance_id, state):
        try:
            self.manilaclient.share_instances.reset_state(share_instance_id, state)
        except Exception as e:
            log.exception("share_instance_reset_state(share_instance_id=%s, state=%s): %s",
                          share_instance_id, state, e)

    def share_snapshot_reset_state(self, snapshot_id, state):
        try:
            self.manilaclient.share_snapshots.reset_state(snapshot_id, state)
        except Exception as e:
            log.exception("share_snapshot_reset_state(snapshot_id=%s, state=%s): %s",
                          snapshot_id, state, e)

    def share_snapshot_instance_reset_state(self, snapshot_instance_id, state):
        try:
            self.manilaclient.share_snapshot_instances.reset_state(snapshot_instance_id, state)
        except Exception as e:
            log.exception("share_snapshot_instance_reset_state(snapshot_instance_id=%s, state=%s): %s",
                          snapshot_instance_id, state, e)

    def list_shares(self, status=None):
        try:
            search_opts = {'all_tenants': 1}
            if status:
                search_opts.update({'status': status})

            opts = copy.deepcopy(search_opts)
            opts.update({'with_count': True})
            _, count = self.manilaclient.shares.list(search_opts=opts)
            count = (count / 1000) + 1

            shares = []
            offset = 0
            for i in range(int(count)):
                search_opts.update({'offset': offset, 'limit': 1000})
                shares.append(self.manilaclient.shares.list(search_opts=search_opts))
                offset = offset + 1000
            return shares
        except Exception as e:
            log.exception("list_shares: %s", e)

    def list_share_instances(self, share_id):
        try:
            return self.manilaclient.shares.list_instances(share_id)
        except Exception as e:
            log.exception("list_share_instances(share_id=%s): %s", share_id, e)

    def list_share_snapshots(self, status=None):
        try:
            search_opts = {'all_tenants': 1}
            if status:
                search_opts.update({'status': status})

            opts = copy.deepcopy(search_opts)
            opts.update({'with_count': True})
            _, count = self.manilaclient.share_snapshots.list(search_opts=opts)
            count = (count / 1000) + 1

            share_snapshots = []
            offset = 0
            for i in range(int(count)):
                search_opts.update({'offset': offset, 'limit': 1000})
                snaps = self.manilaclient.share_snapshots.list(search_opts=search_opts)
                if snaps:
                    share_snapshots.append(snaps)
                    offset = offset + 1000
                else:
                    break
            return share_snapshots
        except Exception as e:
            log.exception("list_share_snapshots: %s", e)

    def list_share_snapshot_instances(self, snapshot_id):
        try:
            return self.manilaclient.share_snapshot_instances.list(snapshot=snapshot_id, detailed=True)
        except Exception as e:
            log.exception("list_share_snapshot_instances(snapshot_id=%s): %s", snapshot_id, e)

    def share_delete(self, share_id):
        try:
            self.manilaclient.shares.delete(share_id)
        except Exception as e:
            log.exception("share_delete(share_id=%s): %s", share_id, e)

    def share_replica_delete(self, replica_id):
        try:
            self.manilaclient.share_replicas.delete(replica_id)
        except Exception as e:
            log.exception("share_replica_delete(replica_id=%s): %s", replica_id, e)

    def share_snapshot_delete(self, snapshot_id):
        try:
            self.manilaclient.share_snapshots.delete(snapshot_id)
        except Exception as e:
            log.exception("share_snapshot_delete(snapshot_id=%s): %s",
                          snapshot_id, e)

    def share_force_delete(self, share_id):
        try:
            self.manilaclient.shares.force_delete(share_id)
        except Exception as e:
            log.exception("share_force_delete(share_id=%s): %s", share_id, e)

    def share_instance_force_delete(self, share_instance_id):
        try:
            self.manilaclient.share_instances.force_delete(share_instance_id)
        except Exception as e:
            log.exception("share_instance_force_delete(share_instance_id=%s): %s",
                          share_instance_id, e)

    def share_snapshot_force_delete(self, snapshot_id):
        try:
            self.manilaclient.share_snapshots.force_delete(snapshot_id)
        except Exception as e:
            log.exception("share_snapshots_force_delete(snapshot_id=%s): %s",
                          snapshot_id, e)

    def get_share_host(self, share_id):
        try:
            share = self.manilaclient.shares.get(share_id)
            return share.host
        except Exception as e:
            log.exception("get_share_host(share_id=%s): %s", share_id, e)

    def query_shares_with_affinity_rules(self):
        with self.db_session as sess:
            shares_t = self.db_table('shares')
            share_metadata_t = self.db_table('share_metadata')
            stmt = (select(share_metadata_t.c.share_id, share_metadata_t.c.value.label('rule'))
                    .join(shares_t, shares_t.c.id == share_metadata_t.c.share_id)
                    .where(shares_t.c.deleted == 'False')
                    .where(share_metadata_t.c.deleted == 0)
                    .where(share_metadata_t.c.key == '__affinity_same_host'))  # yapf: disable
            shares = sess.execute(stmt).all()
            return shares

    def query_shares_with_anti_affinity_rules(self):
        with self.db_session as sess:
            shares_t = self.db_table('shares')
            share_metadata_t = self.db_table('share_metadata')
            stmt = (select(share_metadata_t.c.share_id, share_metadata_t.c.value.label('rule'))
                    .join(shares_t, shares_t.c.id == share_metadata_t.c.share_id)
                    .where(shares_t.c.deleted == 'False')
                    .where(share_metadata_t.c.deleted == 0)
                    .where(share_metadata_t.c.key == '__affinity_different_host'))  # yapf: disable
            shares = sess.execute(stmt).all()
            return shares

    def query_share_host(self, share_id):
        """ Query share host by share id

            :param string share_id: share id
            :return dict share: {'share_id': *, 'host': *}
        """
        with self.db_session as sess:
            share_instances_t = self.db_table('share_instances')
            stmt = (select(share_instances_t.c.host)
                    .where(share_instances_t.c.share_id == share_id)
                    .where(share_instances_t.c.deleted == 'False')
                    .where(share_instances_t.c.replica_state == 'active'))  # yapf: disable
            result = sess.execute(stmt).first()
            return {'share_id': share_id, 'host': result[0]} if result else {'share_id': share_id}


def create_manila_client(config_file, version):
    """  Parse config file and create manila client

        :param string config_file:
        :return client.Client manila:  manila client
    """
    try:
        parser = configparser.ConfigParser()
        parser.read(config_file)
        auth_url = parser.get('keystone_authtoken', 'www_authenticate_uri')
        username = parser.get('keystone_authtoken', 'username')
        password = parser.get('keystone_authtoken', 'password')
        user_domain = parser.get('keystone_authtoken', 'user_domain_name')
        prj_domain = parser.get('keystone_authtoken', 'project_domain_name')
        prj_name = parser.get('keystone_authtoken', 'project_name')
    except Exception as e:
        print(f"ERROR: Parse {config_file}: " + str(e))
        sys.exit(2)

    auth = v3.Password(
        username=username,
        password=password,
        user_domain_name=user_domain,
        project_domain_name=prj_domain,
        project_name=prj_name,
        auth_url=auth_url,
    )
    sess = session.Session(auth=auth)
    manila = client.Client(version, session=sess)
    return manila


def str2bool(val):
    if isinstance(val, bool):
        return val
    if val.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if val.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    raise argparse.ArgumentTypeError('Boolean value expected.')

def response(func):
    def wrapper_func(self, *args, **kwargs):
        try:
            status_code = 200
            header = ('Content-Type', 'application/json')
            data = func(self, *args, **kwargs)
            data = json.dumps(data, indent=4, sort_keys=True, default=str)
            return status_code, header, data
        except Exception as e:
            status_code = 500
            header = ('Content-Type', 'text/html; charset=UTF-8')
            message_parts = [
                repr(e),
            ]
            message = '\r\n'.join(message_parts)
            return status_code, header, message
    return wrapper_func

def update_dict(target_dict, new_dict):
    old_dict = target_dict
    target_dict = {}
    for key in new_dict:
        if key in old_dict:
            target_dict[key] = old_dict[key]
        else:
            target_dict[key] = new_dict[key]
    return target_dict

def update_records(old, new):
    result = {}
    for k in new:
        if k in old:
            result[k] = old[k]
        else:
            result[k] = new[k]
            result[k]['since'] = datetime.datetime.utcnow()
    return result


def is_utcts_recent(ts: datetime.datetime, seconds):
    delta = datetime.datetime.utcnow() - ts
    if delta.total_seconds() < seconds:
        return True
    return False

class CustomAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        task = kwargs.pop('task', self.extra.get('task', None))
        if task:
            msg = f'{task}: {msg}'
        dry_run = kwargs.pop('dry_run', self.extra.get('dry_run', False))
        if dry_run:
            msg = f'[DRY-RUN] {msg}'
        for k, v in kwargs.items():
            msg += f' {k}={v}'
        return msg, {}
