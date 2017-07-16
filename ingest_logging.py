import logging
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from datetime import datetime
from time import time
from requests_aws4auth import AWS4Auth
from json import dumps


class ElasticsearchSink:
    def __init__(self):
        self.today = datetime.strftime(datetime.today(), "%Y-%m-%d")
        self.indexName = "logging-{}-{}"
        self.docType = "tests"
        self.theport = 80
        self.ACCESS_KEY = "AKIAIAOZRRLDX37HKW4Q"
        self.SECRET_KEY = "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"
        self.REGION = "eu-west-1"
        self.host = 'search-shippy-es-f5eynamiumiunz5mrdxxmodksu.eu-west-1.es.amazonaws.com'
        self.awsauth = AWS4Auth(self.ACCESS_KEY, self.SECRET_KEY, self.REGION, 'es')
        self.es = self.set_es()
        self.data_buffer = []

        # Logging
        self.loglevel = logging.DEBUG
        self.logger = logging.getLogger()
        self.logger.setLevel(self.loglevel)

    def set_es(self):
        return Elasticsearch(
            hosts=[{'host': self.host, 'port': self.theport}],
            http_auth=self.awsauth,
            connection_class=RequestsHttpConnection
        )

    def add_data(self, data):
        if data is None:
            return
        self.data_buffer.append(data)

    def get_index_name(self, version):
        return self.indexName.format(version, self.today)

    def bulk_index(self):

        tobulkindex = []

        self.logger.debug("Indexing {} events".format(len(self.data_buffer)))
        self.logger.debug("The events are following")
        self.logger.debug(dumps(self.data_buffer))

        for ev in self.data_buffer:
            ev['_index'] = self.get_index_name(ev['dataModelVersion'])
            ev['_type'] = self.docType
            ev['_op_type'] = 'index'
            tobulkindex.append(ev)

        successes, errors = helpers.bulk(self.es, tobulkindex)
        self.logger.debug("Successful bulk operations {}".format(successes))
        self.logger.error("Non successful bulk operations {}".format(errors))


class Enricher:
    def __init__(self, metadata):
        self.devId = metadata['params']['header']['deviceID']
        self.devType = metadata['params']['header']['deviceType']
        self.country = metadata['params']['header']["CloudFront-Viewer-Country"]
        self.isTablet = metadata['params']['header']["CloudFront-Is-Tablet-Viewer"]
        self.isMobile = metadata['params']['header']["CloudFront-Is-Mobile-Viewer"]
        self.isDesktop = metadata['params']['header']["CloudFront-Is-Desktop-Viewer"]
        self.requestTime = int(metadata['params']['header']['networkRequestTime'])
        self.requestArrived = int(time() * 1000)
        self.requestFlightTime = self.requestArrived - self.requestTime

    def enrich_event(self, event):

        if "dataModelVersion" not in event:
            return None

        event['requestTime'] = self.requestTime
        event['requestArrived'] = self.requestArrived
        event['requestFlightTime'] = self.requestFlightTime
        event['deviceID'] = self.devId
        event['deviceType'] = self.devType
        event['request_country'] = self.country
        event['isTablet'] = self.isTablet
        event['isMobile'] = self.isMobile
        event['isDesktop'] = self.isDesktop

        return event

    def enrich_events(self, list_events):
        return [self.enrich_event(event) for event in list_events]


def ingest_logging(event, context):
    """
    :param event: contains the event data received by the API.
      Can be dict, list, str, int, float, or NoneType type.
    :param context: context of the Lambda environment. Can be handy
    :return: Not sure yet :)
    """

    enricher = Enricher(event)
    es_sink = ElasticsearchSink()

    [es_sink.add_data(enricher.enrich_events(ev)) for ev in event['body-json']]

    es_sink.bulk_index()