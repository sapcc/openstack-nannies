import os
import sys
import time
import ConfigParser
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from keystoneauth1.identity import v3
from keystoneauth1 import session
from manilaclient import client

class ManilaNanny(object):
    def __init__(self, config_file, interval, dry_run=False):
        self.config_file = config_file
        self.interval = interval
        self.dry_run = dry_run
        self.initDBConnection()
        self.manilaclient = create_manila_client(config_file)

    def _run(self):
        raise Exception('not implemented')

    def run(self):
        while True:
            self._run()
            time.sleep(self.interval)

    def initDBConnection(self):
        """Establish a database connection"""
        db_url = self.get_db_url()
        engine = create_engine(db_url)
        engine.connect()
        Session = sessionmaker(bind=engine)
        self.db_session = Session()
        self.db_metadata = MetaData()
        self.db_metadata.bind = engine
        self.db_base = declarative_base()

    def get_db_url(self):
        """Return the database connection string from the config file"""
        try:
            parser = ConfigParser.SafeConfigParser()
            parser.read(self.config_file)
            db_url = parser.get('database', 'connection', raw=True)
        except Exception as e:
            print("ERROR: Parse {}: ".format(config_file) + str(e))
            sys.exit(2)
        return db_url

def create_manila_client(config_file, version="2.7"):
    """  Parse config file and create manila client

        :param string config_file:
        :return client.Client manila:  manila client
    """
    try:
        parser = ConfigParser.SafeConfigParser()
        parser.read(config_file)
        auth_url = parser.get('keystone_authtoken', 'www_authenticate_uri')
        username = parser.get('keystone_authtoken', 'username')
        password = parser.get('keystone_authtoken', 'password')
        user_domain = parser.get('keystone_authtoken', 'user_domain_name')
        prj_domain = parser.get('keystone_authtoken', 'project_domain_name')
        prj_name = parser.get('keystone_authtoken', 'project_name')
    except Exception as e:
        print "ERROR: Parse {}: ".format(config_file) + e.message
        sys.exit(2)

    auth = v3.Password(
        username = username,
        password = password,
        user_domain_name = user_domain,
        project_domain_name= prj_domain,
        project_name= prj_name,
        auth_url= auth_url,
    )
    sess = session.Session(auth=auth)
    manila = client.Client(version, session=sess)
    return manila


