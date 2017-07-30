#!/usr/bin/env python

from elasticsearch import Elasticsearch, RequestsHttpConnection, client
from json import load, loads, dump, dumps
from requests_aws4auth import AWS4Auth
import logging

class MyElasticSearch:
    """
    This class is intended as a wrapper to ES.
        It initializes all required variables to allow for a direct
        connection to ES.
        It allows to deploy a new version of a given template
        It also allows to delete an older version that is currently deployed
    """

    def __init__(self, local):

        # Logging
        self.loglevel = logging.DEBUG
        logging.basicConfig()
        self.logger = logging.getLogger()
        self.logger.setLevel(self.loglevel)

        self.local = local
        if local:
            self.theport = 9200
            self.host = 'localhost'
            self.es, self.index_client = self.__set_local_es()
        else:
            self.theport = 80
            self.ACCESS_KEY = "AKIAIAOZRRLDX37HKW4Q"
            self.SECRET_KEY = "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"
            self.REGION = "eu-west-1"
            self.host = 'search-shippy-es-f5eynamiumiunz5mrdxxmodksu.eu-west-1.es.amazonaws.com'
            self.awsauth = AWS4Auth(self.ACCESS_KEY, self.SECRET_KEY, self.REGION, 'es')
            self.es, self.index_client = self.__set_es()


    def __check_es_status(self, es):
        if es.info():
            self.logger.debug("  ES is reachable")
            return True
        else:
            return False

    def __set_local_es(self):
        es = Elasticsearch(
            hosts=[{'host': self.host, 'port': self.theport}]
        )
        if self.__check_es_status(es):
            return es, client.IndicesClient(es)
        else:
            raise Exception("Local Elasticsearch is not reachable")

    def __set_es(self):
        es = Elasticsearch(
            hosts=[{'host': self.host, 'port': self.theport}],
            http_auth=self.awsauth,
            connection_class=RequestsHttpConnection
        )
        if self.__check_es_status(es):
            return es, client.IndicesClient(es)
        else:
            raise Exception("Elasticsearch is not reachable")


# es = MyElasticSearch(True)