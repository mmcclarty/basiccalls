import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import config as cfg
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s')
logger.setLevel(logging.INFO)

try:
    #Set retries for a nonresponsive API connection to 3 when a 500 is encountered
    current_session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    current_session.mount('http://', HTTPAdapter(max_retries=retries))
except requests.RequestException as e:
    logger.critical(str(e))


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
        r = current_session.get(url)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            logger.error("Your query parameters are invalid.")
            return None
    except requests.ConnectionError as con_e:
        logger.error(con_e)
    except Exception as e:
        logger.error(e)
    else:
        data_return = r.text
        return data_return





