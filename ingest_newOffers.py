import logging
from elasticsearch import Elasticsearch, helpers
from json import loads
from datetime import datetime

###################
# Parameters
###################
indexName = "offers-%s" % (datetime.strftime(datetime.today(), "%Y-%m-%d"))

docType = "tests"
debug = True
loglevel = logging.DEBUG

cluster_ip = 'http://192.168.178.206/'
theport = 9200
###################

# Establish the connection
es = Elasticsearch(hosts=[cluster_ip], port=theport)
# Set the logger
logger = logging.getLogger()
logger.setLevel(loglevel)

def getDeviceID(header):
    return header['deviceID']

def getDeviceType(header):
    return header['deviceType']

def ingest_logging_data(event, context):
    """
    :param event: contains the event data received by the API.
      Can be dict, list, str, int, float, or NoneType type.
    :param context: context of the Lambda environment. Can be handy
    :return: Not sure yet :)
    """

    count = 0
    devId = getDeviceID(event['headers'])
    devType= getDeviceType(event['headers'])
    topost = []

    for ev in event['body']:
        count += 1
        tmp = ev
        tmp['_op_type'] = 'index'
        tmp['_index'] = indexName
        tmp['_type'] = docType
        tmp['deviceID'] = devId
        tmp['deviceType'] = devType
        tmp['userlatlong'] = {"lat":ev['userLatitude'],"lon":ev['userLongitude']}
        tmp['drop_off_latlong'] = {"lat":ev['drop_off_latitude'],"lon":ev['drop_off_longitude']}
        tmp.pop("userLatitude")
        tmp.pop("userLongitude")
        tmp.pop("drop_off_latitude")
        tmp.pop("drop_off_longitude")

        topost.append(tmp)

        if count >= 100:
            logging.debug("Elements to ingest %s" % (len(topost)))
            Elasticsearch.helpers.bulk(es, topost)
            topost = []

    logging.debug("Final ingest of %s elements" % (len(topost)))

    helpers.bulk(es, topost)
    return 0


if debug:
    infile= open("dummy_offer_data.json","r")
    event_data = loads(infile.read())
    infile.close()
    context = "some bullshit"
    ingest_logging_data(event_data, context)
