import logging
from elasticsearch import Elasticsearch, RequestsHttpConnection
from json import loads
from datetime import datetime
from time import time
from requests_aws4auth import AWS4Auth
from json import dumps

###################
# Parameters
###################
indexName = "offers-%s" % (datetime.strftime(datetime.today(), "%Y-%m-%d"))
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

def getDeviceID(header):
    return header['deviceID']

def getDeviceType(header):
    return header['deviceType']

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
    if tmp_data['size_packages'] == "Small":
        tmp_data['size_packages'] = 1
    elif tmp_data['size_packages'] == "Medium":
        tmp_data['size_packages'] = 2
    else :
        tmp_data['size_packages'] = 3

    return (tmp_data)

def ingest_newOffers(event, context):
    """
    :param event: contains the event data received by the API.
      Can be dict, list, str, int, float, or NoneType type.
    :param context: context of the Lambda environment. Can be handy
    :return: Not sure yet :)
    """

    # Parse the headers
    devId = event['params']['header']['deviceID']
    devType = event['params']['header']['deviceType']
    # Some CloudFront info that could be interesting
    country = event['params']['header']["CloudFront-Viewer-Country"]
    isTablet = event['params']['header']["CloudFront-Is-Tablet-Viewer"]
    isMobile = event['params']['header']["CloudFront-Is-Mobile-Viewer"]
    isDesktop = event['params']['header']["CloudFront-Is-Desktop-Viewer"]
    requestTime = int(event['params']['header']['networkRequestTime'])
    requestArrived = int(time() * 1000)
    requestFlightTime = requestArrived - requestTime

    # Normalize the data
    logging.debug("event = " + dumps(event['body-json']))
    logging.debug("event class= " + str(event['body-json'].__class__))
    # Just to avoid writing into the ev variable
    tmp = normalizeData(event['body-json'])
    # tmp['_op_type'] = 'index'
    # tmp['_index'] = indexName
    # tmp['_type'] = docType
    tmp['deviceID'] = devId
    tmp['requestArrived'] = requestArrived
    tmp['requestTime'] = requestTime
    tmp['requestFlightTime'] = requestFlightTime
    tmp['request_country'] = country
    tmp['isTablet'] = isTablet
    tmp['isMobile'] = isMobile
    tmp['isDesktop'] =isDesktop
    tmp['pickup_latlong'] = {"lat": tmp['pickup_latitude'],"lon": tmp['pickup_longitude']}
    tmp['drop_off_latlong'] = {"lat": tmp['drop_off_latitude'],"lon": tmp['drop_off_longitude']}
    # For the moment, we decide not to keep the original latitude and longitude
    # so we pop them. No reason but the fact that we would otherwise store twice
    # the same info


    id="{}_{}_{}_{}_{}_{}".format(devId,tmp["pickup_longitude"],tmp["pickup_latitude"],\
                                  tmp["drop_off_latitude"],tmp["drop_off_longitude"],tmp['pickup_date'])

    tmp.pop("pickup_longitude")
    tmp.pop("pickup_latitude")
    tmp.pop("drop_off_latitude")
    tmp.pop("drop_off_longitude")

    # Put it in ES
    es.index(index=indexName, doc_type=docType, id=id, body=tmp)

    return {"acknowledged":True}
