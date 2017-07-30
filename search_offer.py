# coding=utf-8
import logging
from elasticsearch import Elasticsearch, RequestsHttpConnection
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

    def __init__(self, debug=False):
        self.today = datetime.strftime(datetime.today(), "%Y-%m-%d")
        self.indexName = ""
        self.docType = "tests"

        if debug:
            self.theport = 9200
            self.host = "localhost"
        else:
            self.theport = 80
            self.ACCESS_KEY = "AKIAIAOZRRLDX37HKW4Q"
            self.SECRET_KEY = "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"
            self.REGION = "eu-west-1"
            self.host = 'search-shippy-es-f5eynamiumiunz5mrdxxmodksu.eu-west-1.es.amazonaws.com'
            self.awsauth = AWS4Auth(self.ACCESS_KEY, self.SECRET_KEY, self.REGION, 'es')
            self.data_buffer = ""
        self.es = self.__set_es(debug)

        self.es_response_time = 0

        # Logging
        self.loglevel = logging.DEBUG
        logging.basicConfig()
        self.logger = logging.getLogger()
        self.logger.setLevel(self.loglevel)

    def __set_es(self, debug):
        if debug:
            return Elasticsearch(
                hosts=[{'host': self.host, 'port': self.theport}]
            )
        else:
            return Elasticsearch(
                hosts=[{'host': self.host, 'port': self.theport}],
                http_auth=self.awsauth,
                connection_class=RequestsHttpConnection)

    def __get_index_name(self):
            return self.indexName.format(self.today)

    def index(self):
        self.logger.info("Indexing search event")
        self.logger.debug("The event is {}".format(dumps(self.data_buffer)))

        index_res = self.es.index(index=self.__get_index_name(),
                                  doc_type=self.docType, body=self.data_buffer)

        if "acknowledged" in index_res and index_res['acknowledged']:
            self.logger.info("Successful indexing operations")
        elif "result" in index_res and index_res['result'] == "created":
            self.logger.info("Successful indexing operations")
        else:
            self.logger.error("Non successful bulk operations")

    def search(self, body):
        res = self.es.search(index=self.indexName, doc_type=self.docType, body=body)

        self.es_response_time = res['took']

        if res['hits']['total'] == 0:
            return []

        return [ResultFormatter(k["_source"]).format() for k in res['hits']['hits']]


class QueryBuilder:
    def __init__(self, request):
        self.number_packages = request['NumberPackages']
        self.package_size = request['PackageSize']

        self.pickup_date = request['PickupDate']

        # This is the radius we allow for the geo-search
        self.search_radius = "50km"

        self.pickup_latitude = request['PickupLatitude']
        self.pickup_longitude = request["PickupLongitude"]

        self.dropoff_longitude = request["DropoffLongitude"]
        self.dropoff_latitude = request["DropoffLatitude"]

        self.query = {"query": {"bool": {}}}

    def __build_must_query(self):
        return [
            {"range": {"PickupDate": {"gte": self.pickup_date}}},
            {"range": {"NumberPackages": {"gte": self.number_packages}}},
            {"range": {"PackageSize": {"gte": self.package_size}}}
        ]

    def __build_filter_query(self):
        return [
            {
                "geo_distance": {
                    "distance": self.search_radius,
                    "DropoffLatlong": {"lat": self.dropoff_latitude, "lon": self.dropoff_longitude}
                }
            },
            {
                "geo_distance": {
                    "distance": self.search_radius,
                    "PickupLatLng": {"lat": self.pickup_latitude, "lon": self.pickup_longitude}
                }
            }
        ]

    def build_query(self):
        self.query['query']['bool']["must"] = self.__build_must_query()
        self.query['query']['bool']['filter'] = self.__build_filter_query()

        return self.query

    def get_dropoff_object(self):
        return {"lat": self.dropoff_latitude, "lon": self.dropoff_longitude}

    def get_pickup_object(self):
        return {"lat": self.pickup_latitude, "lon": self.pickup_longitude}


class ResultFormatter:
    def __init__(self, es_response):
        self.es_response = es_response

    def __format_latlon(self):
        self.es_response["DropoffLongitude"] = self.es_response['PickupLatLng']['lat']
        self.es_response["DropoffLatitude"] = self.es_response['PickupLatLng']['lon']
        self.es_response["PickupLongitude"] = self.es_response['DropoffLatlong']['lat']
        self.es_response["PickupLatitude"] = self.es_response['DropoffLatlong']['lon']

    def format(self):
        self.__format_latlon()

        return self.es_response


class Reporter:
    def __init__(self, queryBuilder, es_sink, event, arrival_time, number_results):
        # Time it took for ES to return a result
        self.es_response_time = es_sink.es_response_time

        self.es = es_sink
        self.es.indexName = "search-{}"

        # Time when the handler was called
        self.request_arrived = arrival_time
        # Timestamp from the user's smartphone
        self.requestTime = int(event['params']['header']['networkRequestTime'])
        # Request flight time
        self.requestFlightTime = int(self.request_arrived - self.requestTime)

        self.dropoff_latlng = queryBuilder.get_dropoff_object()
        self.pickup_latlng = queryBuilder.get_pickup_object()
        self.pickup_date = queryBuilder.pickup_date
        self.packageSize = queryBuilder.package_size

        self.number_results = number_results

        self.country = event['params']['header']["CloudFront-Viewer-Country"]
        self.isTablet = event['params']['header']["CloudFront-Is-Tablet-Viewer"]
        self.isMobile = event['params']['header']["CloudFront-Is-Mobile-Viewer"]
        self.isDesktop = event['params']['header']["CloudFront-Is-Desktop-Viewer"]
        self.travelBy = event['body-json'][0]["TravelBy"]
        self.devId = event['params']['header']['deviceID']

    def report(self):
        body = {
            "ES_response_time": self.es_response_time,
            "requestArrived": self.request_arrived,
            "requestTime": self.requestTime,
            "requestFlightTime": self.requestFlightTime,
            "DropoffLatlong": self.dropoff_latlng,
            "PickupLatLng": self.pickup_latlng,
            "PickupDate": self.pickup_date,
            'deviceID': self.devId,
            'request_country': self.country,
            'isTablet': self.isTablet,
            'isMobile': self.isMobile,
            'isDesktop': self.isDesktop,
            "numberResults": self.number_results,
            'TravelBy': self.travelBy,
            "PackageSize": self.packageSize,
        }
        self.es.data_buffer = body
        self.es.index()


def search_offer(event, context):

    # Log the time when the request arrives
    arrival_time = int(1000*time())

    query = QueryBuilder(event['body-json'][0])
    #TODO remove the debug!
    es = ElasticsearchSink(True)
    es.indexName = "offers-01-*"
    res = es.search(query.build_query())

    Reporter(query, es, event, arrival_time, len(res)).report()

    return res