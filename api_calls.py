import requests
import config as cfg
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s')
logger.setLevel(logging.INFO)

def fetch_data(cbsa):
    """

    :param values:
    :return:
    """

    api_key = cfg.api_key
    timerange = cfg.timerange
    values = 'HirAEndR,FrmJbGnS,EarnHirAS,EarnHirNS&for=county:' \
             + str(cfg.cbsas[cbsa]['county']) + '&in=state:' + str(cfg.cbsas[cbsa]['state']) \
             + '&time=from ' + timerange + '&sex=0&agegrp=A00&race=A0&' \
             'education=E0&ethnicity=A0&key=' + api_key
    url = cfg.census_host + values

    try:
        r = requests.get(url)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            logger.error(e)
            return None
    except requests.ConnectionError as con_e:
        logger.error(con_e)
    except Exception as e:
        logger.error(e)
    else:
        data_return = r.text
        return data_return





