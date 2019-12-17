import time
import ConfigParser
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

class ManilaNanny(object):
    def __init__(self, db_url, interval, dry_run): 
        self.db_url = db_url
        self.interval = interval
        self.dry_run = dry_run
        self.makeConnection()

    def _run(self):
        raise Exception('not implemented')

    def run(self):
        while True:
            self._run()
            time.sleep(self.interval)

    def makeConnection(self):
        "Establish a database connection and return the handle"
        engine = create_engine(self.db_url)
        engine.connect()
        Session = sessionmaker(bind=engine)
        self.db_session = Session()
        self.db_metadata = MetaData()
        self.db_metadata.bind = engine
        self.db_base = declarative_base()

def get_db_url(config_file):
    """Return the database connection string from the config file"""
    parser = ConfigParser.SafeConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection', raw=True)
    except:
        print "ERROR: Check Manila configuration file."
        sys.exit(2)
    return db_url

