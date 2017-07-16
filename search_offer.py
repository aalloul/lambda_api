import logging
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from json import loads
from datetime import datetime
from time import time
from requests_aws4auth import AWS4Auth
from json import dumps

###################
# Parameters
###################
indexName = "offers-*"
docType = "tests"
loglevel = logging.DEBUG
theport = 80
ACCESS_KEY = "AKIAIAOZRRLDX37HKW4Q"
SECRET_KEY = "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"
REGION = "eu-west-1"
host = 'search-shippy-es-f5eynamiumiunz5mrdxxmodksu.eu-west-1.es.amazonaws.com'
awsauth = AWS4Auth(ACCESS_KEY, SECRET_KEY, REGION, 'es')
###################

# Establish the connection
es = Elasticsearch(
    hosts=[{'host': host, 'port': theport}],
    http_auth=awsauth,
    connection_class=RequestsHttpConnection
)


# Set the logger
logger = logging.getLogger()
logger.setLevel(loglevel)

def normalizeData(data):
        """
        For the moment the app is such that the received values are all strings, so
        we developed this function to cast them to integers or floats (depending)
        """
        tmp_data = data
        logging.debug("drop_off_longitude = " + str(tmp_data['drop_off_longitude']))
        tmp_data['drop_off_longitude'] = float(tmp_data['drop_off_longitude'])
        tmp_data['drop_off_latitude'] = float(tmp_data['drop_off_latitude'])
        tmp_data['pickup_longitude'] = float(tmp_data['pickup_longitude'])
        tmp_data['pickup_latitude'] = float(tmp_data['pickup_latitude'])
        tmp_data['number_packages'] = int(tmp_data['number_packages'])
        tmp_data['pickup_date'] = int(tmp_data['pickup_date'])

        return (tmp_data)

def build_search_body(params):
    """
    This function builds the search body such that the result is relevant if the
    returned results require
      - at least a package as large as the searched one
      - a number of packages equal or larger than requested
      - the pick-up date is at least equal to the requested
      - the pick-up and drop-off cities should match exactly
    """
    thebody = {}
    thebody['query'] = {}
    thebody['query']['bool'] = {}
    thebody['query']['bool']['must'] = []

    if params['size_packages'] == "Small":
        thebody['query']['bool']['must'].append(
            {"range":{"size_packages":{"gte":1}}}
            )

    if params['size_packages'] == "Medium":
        thebody['query']['bool']['must'].append(
            {"range":{"size_packages":{"gte":2}}}
            )

    if params['size_packages'] == "Large":
        thebody['query']['bool']['must'].append(
            {"range":{"size_packages":{"gte":3}}}
            )

    thebody['query']['bool']['must'].append(
        {"range":{"size_packages":{"gte":params['number_packages']}}}
    )
    thebody['query']['bool']['must'].append(
        {"range":{"pickup_date":{"gte":params['pickup_date']}}}
    )
    thebody['query']['bool']['must'].append(
        {"term":{"pickup_city":params['pickup_city']}}
    )
    thebody['query']['bool']['must'].append(
        {"term":{"pickup_country":params['pickup_country']}}
    )
    thebody['query']['bool']['must'].append(
        {"term":{"drop_off_city":params['drop_off_city']}}
    )
    thebody['query']['bool']['must'].append(
        {"term":{"drop_off_country":params['drop_off_country']}}
    )


    return thebody

def return_response(es_res):
    """
    We use this function to return only the fields that are important to the user
    The results are appended to a list and "stringified" using json.dumps
    """
    out = []
    for res in es_res:
        out.append(res['_source'])
    return out

def search_offer(event, context):
    """
    fields of interest are
    pickup_date, number_packages, size_packages, drop_off_latitude, drop_off_longitude
    pickup_latitude, pickup_longitude
    """

    search_params = normalizeData(event['body-json'])

    search_body = build_search_body(search_params)
    res = es.search(index = indexName, doc_type = docType, body = search_body, size = 30 )

    if (res['hits']['total'] == 0):
        return {}

    return return_response(res['hits']['hits'])
