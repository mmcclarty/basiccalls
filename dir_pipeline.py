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
import csv
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

            blanks = []
            for i in range(0, len(table_columns)-2):
                blanks.append('?')

            sql = "INSERT INTO " + str(table_name) + \
                  "(%s)" % ", ".join(table_columns) + " VALUES ('" + last_update + "', '" + cbsa_key + "', "" \
                  ""%s)" % ", ".join(blanks)

            ready_data = [tuple(c) for c in raw_data[1:]]

            try:
                cursor.executemany(sql, ready_data)
                conn.commit()
            except Error as sqlerror:
                logger.error("Data could not be written: " + str(sqlerror))
            else:
                cursor.close()
                logger.info("Data successfully written to table {}".format(table_name))
                return table_name, table_columns


def write_to_csv(conn, table_name, table_columns):
    """

    :return:
    """

    cursor = conn.cursor()
    nullstatements = ["IFNULL({}, '')".format(i) for i in table_columns]
    sql_nonull = "SELECT " + "%s" % ", ".join(nullstatements) + " FROM " + table_name
    try:
        cursor.execute(sql_nonull)
        data = cursor.fetchall()
        print(data)
    except Error as e:
        logger.error(e)
    else:
        writer = csv.writer(open('{}.csv'.format(table_name), "w"))
        writer.writerows(data)


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
                table_name, table_columns = populate_db(raw_data, key, conn)
                write_to_csv(conn, table_name, table_columns)
    else:
        logger.critical("Data could not be written.")


main()
