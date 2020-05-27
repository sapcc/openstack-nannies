from __future__ import absolute_import

import configparser
import http.server
import json
import sys
import time
from threading import Thread

from keystoneauth1 import session
from keystoneauth1.identity import v3
from manilaclient import client
from prometheus_client import start_http_server
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class ManilaNanny(http.server.HTTPServer):
    ''' Manila Nanny '''
    def __init__(self, config_file, interval, dry_run=False, prom_port=0, address="", port=8000, handler=None):
        self.config_file = config_file
        self.interval = interval
        self.dry_run = dry_run
        self.init_db_connection()
        self.manilaclient = create_manila_client(config_file)

        if prom_port != 0:
            try:
                start_http_server(prom_port)
            except Exception as e:
                sys.stdout.write("prometheus_client:start_http_server: " + str(e) + "\n")
                sys.exit(-1)

        # Initialize the class as http server. The handler needs to access the variables
        # in the class
        if handler:
            super(ManilaNanny, self).__init__((address, port), handler)
            thread = Thread(target=self.serve_forever, args=())
            thread.setDaemon(True)
            thread.start()

    def _run(self):
        raise Exception('not implemented')

    def run(self):
        while True:
            self._run()
            time.sleep(self.interval)

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

    def renew_manila_client(self):
        self.manilaclient = create_manila_client(self.config_file, "2.7")

    def undefined_route(self, route):
        status_code = 500
        header = ('Content-Type', 'text/html; charset=UTF-8')
        message_parts = [
            f'{route} is not defined in {self.__class__}',
        ]
        message = '\r\n'.join(message_parts)
        return status_code, header, message


def create_manila_client(config_file, version="2.7"):
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
                str(e),
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
