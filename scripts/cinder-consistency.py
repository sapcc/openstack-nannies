#!/usr/bin/env python
#
# Copyright (c) 2017 CERN
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# this script checks for volume attachments of already deleted volumes in the cinder db

import argparse
import sys
import ConfigParser
import logging

from prettytable import PrettyTable
from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import join
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import false
from sqlalchemy.ext.declarative import declarative_base

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

def get_wrong_volume_attachments(meta):

    """Return a dict indexed by volume_attachment_id and with the value volume_id for non deleted volume_attachments"""

    wrong_attachments = {}
    volume_attachment_t = Table('volume_attachment', meta, autoload=True)
    volumes_t = Table('volumes', meta, autoload=True)
    attachment_join = volume_attachment_t.join(volumes_t,volume_attachment_t.c.volume_id == volumes_t.c.id)
    wrong_volume_attachment_q = select(columns=[volumes_t.c.id,volumes_t.c.deleted,volume_attachment_t.c.id,volume_attachment_t.c.deleted]).select_from(attachment_join).where(and_(volumes_t.c.deleted == "true",volume_attachment_t.c.deleted == "false"))

    for (volume_id, volume_deleted, volume_attachment_id, volume_attachment_deleted) in wrong_volume_attachment_q.execute():
        wrong_attachments[volume_attachment_id] = volume_id
    return wrong_attachments

def fix_wrong_volume_attachments(meta, wrong_attachments):

    """Return a dict indexed by volume_attachment_id and with the value volume_id for non deleted volume_attachments"""

    volume_attachment_t = Table('volume_attachment', meta, autoload=True)

    for volume_attachment_id in wrong_attachments:
        print "- volume attachment id to be deleted: " + volume_attachment_id
        delete_volume_attachment_q = volume_attachment_t.delete().where(volume_attachment_t.c.id == volume_attachment_id)
        delete_volume_attachment_q.execute()
    return wrong_attachments

def makeConnection(db_url):

    """Establish a database connection and return the handle"""

    engine = create_engine(db_url)
    engine.connect()
    Session = sessionmaker(bind=engine)
    thisSession = Session()
    metadata = MetaData()
    metadata.bind = engine
    Base = declarative_base()
    tpl = thisSession, metadata, Base
    return tpl

def get_db_url(config_file):

    """Return the database connection string from the config file"""

    parser = ConfigParser.SafeConfigParser()
    try:
        parser.read(config_file)
        db_url = parser.get('database', 'connection', raw=True)
    except:
        print "ERROR: Check Cinder configuration file."
        sys.exit(2)
    return db_url

# cmdline handling
def parse_cmdline_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        default='./cinder.conf',
                        help='configuration file')
    parser.add_argument("--dry-run",
                       action="store_true",
                       help='print only what would be done without actually doing it')
    return parser.parse_args()

def main():
    try:
        args = parse_cmdline_args()
    except Exception as e:
        sys.stdout.write("Check command line arguments (%s)" % e.strerror)

    # connect to the DB
    db_url = get_db_url(args.config)
    cinder_session, cinder_metadata, cinder_Base = makeConnection(db_url)

    wrong_attachments = get_wrong_volume_attachments(cinder_metadata)
    if len(wrong_attachments) != 0:
        print "- volume attachment inconsistencies found"
        # print out what we would delete
        ptable = PrettyTable(["volume_attachment_id", "deleted volume_id"])
        for volume_attachment_id in wrong_attachments:
            ptable.add_row([volume_attachment_id,wrong_attachments[volume_attachment_id]])
        print ptable
        if not args.dry_run:
            print "- deleting volume attachment inconsistencies found"
            deleted_attachments = fix_wrong_volume_attachments(cinder_metadata, wrong_attachments)
            # print out what we will delete
            ptable = PrettyTable(["deleted attachment_id", "deleted volume_id"])
            for deleted_attachment_id in deleted_attachments:
                ptable.add_row([deleted_attachment_id, deleted_attachments[deleted_attachment_id]])
            print ptable
    else:
        print "- volume attachments are consistent"

if __name__ == "__main__":
    main()
