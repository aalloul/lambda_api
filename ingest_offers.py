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
        # Reduced to yearly based to reduce the explosion of indices while we have
        # a ver small number of offers
        self.today = datetime.strftime(datetime.today(), "%Y")
        self.indexName = "offers-{}-{}"
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
        logging.basicConfig()
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

        self.logger.info("Indexing {} events".format(len(self.data_buffer)))
        self.logger.debug("The events are following")
        self.logger.debug(dumps(self.data_buffer))

        for ev in self.data_buffer:
            ev['_index'] = self.__get_index_name(ev['dataModelVersion'])
            ev['_type'] = self.docType
            ev['_op_type'] = 'index'
            tobulkindex.append(ev)

        successes, errors = helpers.bulk(self.es, tobulkindex)
        self.logger.info("Successful bulk operations {}".format(successes))
        self.logger.error("Non successful bulk operations {}".format(errors))

    def index(self):
        self.logger.info("Indexing {} events".format(len(self.data_buffer)))
        self.logger.debug("The events are following")
        self.logger.debug(dumps(self.data_buffer))

        successes = 0
        errors = 0

        for ev in self.data_buffer:
            index_res = self.es.index(index=self.__get_index_name(ev["offerVersion"]),
                                      doc_type=self.docType,
                                      body=ev)
            if "acknowledged" in index_res and index_res['acknowledged']:
                successes += 1
            elif "result" in index_res and index_res['result'] == "created":
                successes += 1
            else:
                errors += 1

        self.logger.info("Successful indexing operations {}/{}".format(successes, len(self.data_buffer)))
        self.logger.error("Non successful bulk operations {}/{}".format(errors, len(self.data_buffer)))


class Enricher:
    """
    This class serves to enrich the data using the header
    parameters transferred by the API endpoint.
    It is initialized using by passing in the event variable
    that is passed onto the handler function.
    """

    def __init__(self, event):
        self.devId = event['params']['header']['deviceID']
        self.devType = event['params']['header']['deviceType']

        if 'offerVersion' in event['params']['header']:
            self.offerVersion = float(event['params']['header']['offerVersion'])
        else:
            self.offerVersion = None

        self.country = event['params']['header']["CloudFront-Viewer-Country"]
        self.isTablet = event['params']['header']["CloudFront-Is-Tablet-Viewer"]
        self.isMobile = event['params']['header']["CloudFront-Is-Mobile-Viewer"]
        self.isDesktop = event['params']['header']["CloudFront-Is-Desktop-Viewer"]

        self.requestTime = int(event['params']['header']['networkRequestTime'])
        self.requestArrived = int(time() * 1000)
        self.requestFlightTime = self.requestArrived - self.requestTime

        self.event = event['body-json']

    def __add_headers(self, ev):
        """
        Adds the headers to the event's body
        :param ev: The event as received by the API
        :return: An enriched version of the event
        """
        ev['requestTime'] = self.requestTime
        ev['requestArrived'] = self.requestArrived
        ev['requestFlightTime'] = self.requestFlightTime
        ev['deviceID'] = self.devId
        ev['deviceType'] = self.devType
        ev['request_country'] = self.country
        ev['isTablet'] = self.isTablet
        ev['isMobile'] = self.isMobile
        ev['isDesktop'] = self.isDesktop
        # The dataModelVersion is sent as a String by the App. We convert it to a
        # float as it has to be a float
        ev['offerVersion'] = self.offerVersion

        return ev

    def __generateGeoPoint(self, ev):
        """
        Create Geo-Point object and drop redundant fields
        :param ev: Dictionary that should contain the keys
            DropoffLongitude, DropoffLatitude, PickupLongitude, PickupLatitude
        :return: the event with geo-points (nested objects) DropoffLatlong and PickupLatLng.
                 the original keys (*Longitude and *Latitude) are popped.
        """
        ev['DropoffLatlong'] = {"lon": ev['DropoffLongitude'], "lat": ev['DropoffLatitude']}
        ev['PickupLatLng'] = {"lon": ev['PickupLongitude'], "lat": ev['PickupLatitude']}
        ev.pop("DropoffLongitude")
        ev.pop("DropoffLatitude")
        ev.pop("PickupLongitude")
        ev.pop("PickupLatitude")

        return ev

    def __enrich_event(self, ev):
        """
        This method is called for each event.
        :param ev: Single event contained in the body-json of the "event"
        parameter of the handler function.
        :return: a dictionary representing the event + the extra meta data
        """

        myev = self.__generateGeoPoint(ev)
        myev = self.__add_headers(myev)

        return myev

    def enrich_events(self):
        """
        This method operates on a list of dictionaries. It simply calls
        :meth: __enrich_event for each event that is passed.
        :return: A list of events, where each was enriched with meta-data
        """
        return [self.__enrich_event(event) for event in self.event]


def ingest_offer(event, context):
    """
    Handler function called by the Lambda interface.
    :param event: contains the event data received by the API.
      Can be dict, list, str, int, float, or NoneType type.
    :param context: context of the Lambda environment. Can be handy
    :return: None
    """

    enricher = Enricher(event)

    if enricher.offerVersion is None or enricher.offerVersion != 0.1:
        return None

    es_sink = ElasticsearchSink()

    es_sink.add_data(enricher.enrich_events())

    # es_sink.bulk_index()
    es_sink.index()
