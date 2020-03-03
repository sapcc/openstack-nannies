import os
import sys
import time
import ConfigParser
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

class ManilaNanny(object):
    def __init__(self, config_file, interval, dry_run=False):
        self.config_file = config_file
        self.interval = interval
        self.dry_run = dry_run
        self.initDBConnection()

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

