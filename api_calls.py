import requests
import config as cfg
import logging

logger = logging.getLogger()

def fetch_data(values):
    """

    :param values:
    :return:
    """

    api_key = cfg.api_key
    values = 'Emp&for=county:198&in=state:02&year=2012&quarter=1&sex=1&sex=2&agegrp=A02&agegrp=A07' \
             '&ownercode=A05&firmsize=1&seasonadj=U&industry=11&key=' + api_key
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



