import logging
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from datetime import datetime
from time import time
from requests_aws4auth import AWS4Auth
from json import dumps


class ElasticsearchSink:
    """
    This class is intended as a wrapper to ES.
        It initializes all required variables to allow for a direct
        connection to ES.
        It serves also as a data buffer and an interface to the
        bulk API.
    """
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
        self.es = self.__set_es()
        self.data_buffer = []

        # Logging
        self.loglevel = logging.DEBUG
        self.logger = logging.getLogger()
        self.logger.setLevel(self.loglevel)

    def __set_es(self):
        return Elasticsearch(
            hosts=[{'host': self.host, 'port': self.theport}],
            http_auth=self.awsauth,
            connection_class=RequestsHttpConnection
        )

    def add_data(self, data):
        """
        This method adds each line of data to the buffer.
        :param data: This is a dictionary representing the
        data sent by the APP.
        :return: None.
        """
        if data is None:
            return
        self.data_buffer.append(data)

    def __get_index_name(self, version):
        return self.indexName.format(version, self.today)

    def bulk_index(self):
        """
        This method calls the bulk API of ES to ingest the data
        stored in the buffer.
        :return: None
        If the indexing doesn't go well, the output is written to stdout
        """

        tobulkindex = []

        self.logger.debug("Indexing {} events".format(len(self.data_buffer)))
        self.logger.debug("The events are following")
        self.logger.debug(dumps(self.data_buffer))

        for ev in self.data_buffer:
            ev['_index'] = self.__get_index_name(ev['dataModelVersion'])
            ev['_type'] = self.docType
            ev['_op_type'] = 'index'
            tobulkindex.append(ev)

        successes, errors = helpers.bulk(self.es, tobulkindex)
        self.logger.debug("Successful bulk operations {}".format(successes))
        self.logger.error("Non successful bulk operations {}".format(errors))


class Enricher:
    """
    This class serves to enrich the data using the header
    parameters transferred by the API endpoint.
    It is initialized using by passing in the event variable
    that is passed onto the handler function.
    """
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

    def __enrich_event(self, event):
        """
        This method is called for each event.
        :param event: Single event contained in the body-json of the "event"
        parameter of the handler function.
        :return: a dictionary representing the event + the extra meta data
        """
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
        """
        This method operates on a list of dictionaries. It simply calls
        :meth: __enrich_event for each event that is passed.
        :param list_events: This is the list of events as transferred by the API.
        :return: A list of events, where each was enriched with meta-data
        """
        return [self.__enrich_event(event) for event in list_events]


def ingest_logging(event, context):
    """
    Handler function called by the Lambda interface.
    :param event: contains the event data received by the API.
      Can be dict, list, str, int, float, or NoneType type.
    :param context: context of the Lambda environment. Can be handy
    :return: None
    """

    enricher = Enricher(event)
    es_sink = ElasticsearchSink()

    [es_sink.add_data(enricher.enrich_events(ev)) for ev in event['body-json']]

    es_sink.bulk_index()