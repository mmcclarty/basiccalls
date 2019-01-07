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
import config as cfg
import logging
import json
import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s')
logger.setLevel(logging.INFO)


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


def populate_db(raw_data, table_name, conn):
    """

    :return:
    """

    try:
        cursor = conn.cursor()
    except Exception as sqlerror:
        logger.error(sqlerror)
    else:
        table_columns = raw_data[0]

        #Make a new table if the desired one doesn't exist yet
        sql_create = 'CREATE TABLE IF NOT EXISTS ' + str(table_name) + '( `LAST_UPDATE` TEXT, `CBSA` TEXT, `DATETIME` TEXT, `HirAEndR` NUMERIC, `FrmJbGnS` NUMERIC, `EarnHirAs` NUMERIC, `EarnHirNS` NUMERIC, `Sex` INTEGER, `AgeGrp` TEXT, `Education` TEXT, `Race` TEXT, `Ethnicity` TEXT, PRIMARY KEY(`LAST_UPDATE`));'

        try:
            cursor.execute(sql_create)
        except Exception as e:
            logger.error(e)
        else:
            cursor.close()

            last_update = str(datetime.datetime.now())
            # sql = "INSERT INTO " + str(table_name) + " (LAST_UPDATE, CBSA, DATETIME, HirAEndR, FrmJbGnS, EarnHirAs, " \
            #                                          "EarnHirNS, Sex, AgeGrp, Education, Race, Ethnicity), " \
            #                                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            sql = "INSERT INTO " + str(table_name) + " (LAST_UPDATE, CBSA, DATETIME, HirAEndR, FrmJbGnS, EarnHirAs, " \
                                                     "EarnHirNS, Sex, AgeGrp) " \
                                                     "VALUES ('" + last_update + "', 'Dallas', ?, ?, ?, ?, ?, ?, ?)"

    
            ready_data = [tuple(c) for c in raw_data[1:]]

            cursor = conn.cursor()
            cursor.executemany(sql, ready_data)
            cursor.close()


def main():
    """

    :return:
    """

    # Instead of checking for source data updates periodically (which could be thrown off if there is an unexpected
    # crash), on restart the program will check for new data automatically

    conn = connect_to_db(cfg.db_file)

    # Populate Dallas table
    values = ''
    try:
        dallas_raw = json.loads(get_data(values))
    except Exception as e:
        logger.error(e)

    populate_db(dallas_raw, 'DALLAS_EMP_STATS', conn)



main()
