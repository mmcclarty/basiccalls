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


def connect_to_db(db):
    """
    Connect to sqlite database

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
    Hit API to retrieve data of interest

    :param values: cbsa of interest
    :return:
    """

    raw_data = api.fetch_data(values)

    return raw_data


def populate_db(raw_data, cbsa_key, conn):
    """
    Write retrieved data to db. Make a separate table for each cbsa of interest.

    :param raw_data: data
    :param cbsa_key: cbsa of interest
    :param conn: live database connection
    :return:
    """

    table_name = '{}_EMP_STATS'.format(str(cbsa_key))

    # First check if table already exists
    does_exist_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='{}';".format(table_name)

    try:
        cursor = conn.cursor()
    except Error as sqlerror:
        logger.error(sqlerror)
    else:
        cursor.execute(does_exist_sql)
        does_exist = cursor.fetchall()
        print(does_exist)

        if does_exist:
            get_table = "SELECT * FROM {} LIMIT 1".format(table_name)
            cursor.execute(get_table)

            table_columns = raw_data[0]
            table_columns.insert(0, "cbsa")
            table_columns.insert(0, "last_update")
            table_columns.insert(0, "megg")

            #Now check if there are any new columns you need to add to the table to accommodate the new data
            names = [name[0] for name in cursor.description]
            new_columns = list(set(table_columns).difference(names))

            if new_columns:
                alterstatements = ["ALTER TABLE {} ADD {}".format(table_name, i) for i in new_columns]
                alter_table_sql = '%s' % '; '.join(alterstatements)

                cursor.executescript(alter_table_sql)
                print(cursor.description)

            try:
                last_update = str(datetime.datetime.now())

                blanks = []
                for i in range(0, len(table_columns) - 2):
                    blanks.append('?')

                sql = "INSERT INTO " + str(table_name) + \
                      "(%s)" % ", ".join(table_columns) + " VALUES ('" + last_update + "', '" + cbsa_key + "', "" \
                                      ""%s)" % ", ".join(blanks)

                ready_data = [tuple(c) for c in raw_data[1:]]

                cursor.executemany(sql, ready_data)
                conn.commit()
            except Error as sqlerror:
                logger.error("Data could not be written: " + str(sqlerror))
            except Exception as e:
                logger.error(str(e))
            else:
                cursor.close()
                logger.info("Data successfully written to table {}".format(table_name))
                return table_name, table_columns
        else:
            # Make a new table if the desired one doesn't exist yet
            # On conflict replace so that redundant data isn't repeated each time the API is hit
            sql_create = "CREATE TABLE IF NOT EXISTS " + str(table_name) \
                         + "(%s " % ", ".join(table_columns) + ", UNIQUE(%s)" % ", ".join(table_columns[2:]) + \
                         " ON CONFLICT REPLACE)"



def write_to_csv(conn, table_name, table_columns):
    """
    Write produced data to three separate local csv files.

    :param conn: live db connection
    :param table_name: table of interest
    :param table_columns: headers
    :return:
    """

    cursor = conn.cursor()

    # Replacing all NULLS in db with empty strings, as NULLS are sometimes mishandled by clients like visualization tools
    nullstatements = ["IFNULL({}, '') as {}".format(i, i) for i in table_columns]
    sql_nonull = "SELECT " + "%s" % ", ".join(nullstatements) + " FROM " + table_name

    try:
        cursor.execute(sql_nonull)
        data = cursor.fetchall()
    except Error as e:
        logger.error(e)
    else:
        names = [name[0] for name in cursor.description]
        # Create if does not exist, else overwrite (not sustainable for very large datasets.  With more time, would
        # pull last line of csv, do sorted select for only db records newer than that, then append to csv
        try:
            writer = csv.writer(open('{}.csv'.format(table_name), "w"))
            # Write headers from db
            writer.writerow(names)
            # Write data
            writer.writerows(data)
        except csv.Error as csvError:
            logger.error("Problem writing to csv: " + str(csvError))
            return False
        else:
            return True


def main():
    """

    :return:
    """

    # Connect to sqlite db
    conn = connect_to_db(cfg.db_file)

    # For each cbsa specified in configuration file, fetch data from API
    if conn is not None:
        for key in cfg.cbsas.keys():
            try:
                logger.info("Fetching data for {} cbsa".format(str(key)))
                raw_data = json.loads(get_data(key))
            except Exception as e:
                logger.error(e)
            else:
                # Populate the database
                table_name, table_columns = populate_db(raw_data, key, conn)

                # Export to csv
                csv_r = write_to_csv(conn, table_name, table_columns)
    else:
        logger.critical("Data could not be written.")


main()
