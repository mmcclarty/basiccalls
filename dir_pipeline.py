"""
GENERAL PIPELINE

This application will receive data from an external source, transform it to structured storage, and export it to csv
periodically.

"""

__author__ = 'Megan McClarty'
__date__ = 'January 6 2019'
__version__ = '1.0'

import api_calls as api
import sqlite3 as sql
from sqlite3 import Error
import config as cfg
import logging
import json
import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s')
logger.setLevel(logging.INFO)

# TODO: Try and force UPSERT instead of INSERT/REPLACE (Define primary key manually if necessary)
def connect_to_db(db):
    """

    :param db:
    :return:
    """

    try:
        conn = sql.connect(cfg.db_file)
    except sql.Error as e:
        logger.error("There was a problem accessing database: " + str(e))
        return None
    else:
        return conn


def get_data(values):
    """

    :return:
    """

    raw_data = api.fetch_data(values)

    return raw_data


def populate_db(raw_data, cbsa_key, conn):
    """

    :return:
    """

    table_name = '{}_EMP_STATS'.format(str(cbsa_key))

    try:
        cursor = conn.cursor()
    except Error as sqlerror:
        logger.error(sqlerror)
    else:
        table_columns = raw_data[0]
        table_columns.insert(0, "cbsa")
        table_columns.insert(0, "last_update")

        #Make a new table if the desired one doesn't exist yet
        sql_create = "CREATE TABLE IF NOT EXISTS " + str(table_name)\
                     + "(%s " % ", ".join(table_columns) + ", UNIQUE(%s)" % ", ".join(table_columns[2:]) +\
                     " ON CONFLICT REPLACE)"

        try:
            cursor.execute(sql_create)
        except Error as e:
            logger.error(e)
        else:
            last_update = str(datetime.datetime.now())

            sql = "INSERT INTO " + str(table_name) + \
                  "(%s)" % ", ".join(table_columns) + " VALUES ('" + last_update + "', '" + cbsa_key + "', "" \
                  ""?, ?, ?, ?, ?, ?)"

            ready_data = [tuple(c) for c in raw_data[1:]]

            try:
                cursor.executemany(sql, ready_data)
                conn.commit()
            except Error as sqlerror:
                logger.error("Data could not be written: " + str(sqlerror))
            else:
                cursor.close()
                logger.info("Data successfully written to table {}".format(table_name))


def main():
    """

    :return:
    """

    conn = connect_to_db(cfg.db_file)

    # Populate tables
    if conn is not None:
        for key in cfg.cbsas.keys():
            try:
                logger.info("Fetching data for {} cbsa".format(str(key)))
                raw_data = json.loads(get_data(key))
            except Exception as e:
                logger.error(e)
            else:
                populate_db(raw_data, key, conn)
    else:
        logger.critical("Data could not be written.")



main()
