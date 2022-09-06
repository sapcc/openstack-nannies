#!/usr/bin/env python
#
# Copyright (c) 2018 SAP SE
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
# this script checks the instance mappings in queens nova


import sys
import mysql.connector
import ConfigParser
import argparse
import logging
import re


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')

def dsn_to_args(api_db_url):
  db_url_splitted = re.search(r"^.*://(.*):(.*)@(.*)/(.*)\?.*", api_db_url)
  db_url_port = re.search(r"^(.*):(.*)", str(db_url_splitted.group(3)))
  if db_url_port:
    db_port = str(db_url_port.group(2))
    db_host = str(db_url_port.group(1))
  else:
    db_port = '3306'
    db_host = str(db_url_splitted.group(3))
  db_args = {
    'user': str(db_url_splitted.group(1)),
    'password': str(db_url_splitted.group(2)),
    'host': db_host,
    'port': db_port,
    'database': str(db_url_splitted.group(4))
  }
  return db_args


def get_api_db_url(config_file):
  parser = ConfigParser.SafeConfigParser()
  try:
    parser.read(config_file)
    api_db_url = parser.get('api_database', 'connection', raw=True)
  except:
    log.error("ERROR: Check Nova configuration file.")
    sys.exit(2)
  return api_db_url


def _get_conn(db):
  db_args = dsn_to_args(db)
  conn = mysql.connector.connect(**db_args)
  return conn


def parse_cmdline_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("--config",
                      default='./nova.conf',
                      help='configuration file')
  parser.add_argument("--dry-run",
                      action="store_true",
                      help='print only what would be done without actually doing it')
  return parser.parse_args()


try:
  args = parse_cmdline_args()
except Exception as e:
  log.error("Check command line arguments (%s)", e.strerror)

# Connect to databases
api_db_args = dsn_to_args(get_api_db_url(args.config))
api_conn = mysql.connector.connect(**api_db_args)
api_cur = api_conn.cursor(buffered=True)

# Get list of all cells
api_cur.execute("SELECT id, name, database_connection FROM cell_mappings")
CELLS = [{'id': id, 'name': name, 'db': _get_conn(db)} for id, name, db in api_cur.fetchall()]

# Get list of all unmapped instances
api_cur.execute("SELECT instance_uuid FROM instance_mappings WHERE cell_id IS NULL")
log.info("unmapped instances - discovered number: %s", api_cur.rowcount)

# Go over all unmapped instances
unmapped_instances = api_cur.fetchall()
for (instance_uuid,) in unmapped_instances:
  instance_cell = None
  build_request = False

  # Check if a build request exists, if so, skip.
  api_cur.execute("SELECT id FROM build_requests WHERE instance_uuid = %s", (instance_uuid,))
  if api_cur.rowcount != 0:
    log.info("unmapped instances - build request for instance %s exists, checking if instance has been scheduled", instance_uuid)
    build_request = True

  # Check which cell contains this instance
  for cell in CELLS:
    cell['db'].cursor(buffered=True).execute("SELECT id FROM instances WHERE uuid = %s", (instance_uuid,))

    if cell['db'].cursor(buffered=True).rowcount != 0:
      instance_cell = cell
      break

  # Update to the correct cell
  if instance_cell:
    log.warn("unmapped instances - found missing instance mapping to cell %s", instance_cell['id'])
    if not args.dry_run:
      log.info("unmapped instances - fixing missing instance mapping of instance %s to cell %s", instance_uuid, instance_cell['id'])
      log.debug("UPDATE instance_mappings SET cell_id = '%s' WHERE instance_uuid = '%s';", instance_cell['id'], instance_uuid)
      api_cur.execute("UPDATE instance_mappings SET cell_id = '%s' WHERE instance_uuid = '%s';" % (instance_cell['id'], instance_uuid))
      api_conn.commit()
    if build_request:
      if not args.dry_run:
        log.info("unmapped instances - build requests existing for scheduled instance %s, deleting build-request to fix instance-list", instance_uuid)
        api_cur.execute("DELETE FROM build_requests WHERE instance_uuid = %s", (instance_uuid,))
        api_conn.commit()
    continue

  # If we reach this point, it's not in any cell?!
  log.info("unmapped instances - instance %s not found in any cell", instance_uuid)

# Go over all build-requests instances
api_cur.execute("SELECT instance_uuid FROM build_requests")

building_instances = api_cur.fetchall()
for (instance_uuid,) in building_instances:
  api_cur.execute("SELECT id FROM instance_mappings WHERE instance_uuid = %s AND cell_id IS NOT NULL;", (instance_uuid,))
  if api_cur.rowcount != 0:
    log.info("Found build_request of instance that has been already scheduled: %s", instance_uuid)
    if not args.dry_run:
      log.info("deleting build_request of %s", instance_uuid)
      api_cur.execute("DELETE FROM build_requests WHERE instance_uuid = %s", (instance_uuid,))
      api_conn.commit()
