# CBSA of Interest
# I made an assumption here that the cbsas should be defined by their counties.
cbsas = {'DALLAS' : {'state' : '48', 'county' : '113'},
         'ATLANTA' : {'state' : '13', 'county' : '121'},
         'INDIANAPOLIS': {'state': '18', 'county': '121'}}

# Timerange
timerange = '2014 to 2017-Q3'

# Hosts
census_host = 'http://api.census.gov/data/timeseries/qwi/sa?get='
db_file = 'census_db.db'

# Keys and access
api_key = 'e135c4aac56509004edfc2058c172441fd85f050'