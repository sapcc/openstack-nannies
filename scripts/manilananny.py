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
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class ManilaNanny(http.server.HTTPServer):
    def __init__(self, config_file, interval, dry_run=False, address="", port=8000, handler=None):
        self.config_file = config_file
        self.interval = interval
        self.dry_run = dry_run
        self.init_db_connection()
        self.manilaclient = create_manila_client(config_file)
        self.handler = handler
        self.address = address
        self.port = port

    def _run(self):
        raise Exception('not implemented')

    def run(self):
        if self.handler:
            super(ManilaNanny, self).__init__((self.address, self.port), self.handler)
            thread = Thread(target=self.serve_forever, args=())
            thread.setDaemon(True)
            thread.start()
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
