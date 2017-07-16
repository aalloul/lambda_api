#!/usr/bin/env bash
# API key vmXvcob4V33NpNVXKsDll8nAQAcmHGZZ87Fl4HF6

echo "==================================="
echo " Lambda package deployer v0.1"
rm -rf *zip
echo "1- Create the logging package "
zip -q9 ingest_logging_data.zip ingest_logging_data.py
zip -qr9 ingest_logging_data.zip elasticsearch
zip -qr9 ingest_logging_data.zip urllib3
zip -qr9 ingest_logging_data.zip requests_aws4auth
zip -qr9 ingest_logging_data.zip requests
zip -qr9 ingest_logging_data.zip requests-2.13.0.dist-info
zip -qr9 ingest_logging_data.zip requests_aws4auth-0.9.dist-info
zip -qr9 ingest_logging_data.zip urllib3-1.20.dist-info

#echo "  -> Delete current deployment"
#aws lambda delete-function \
#--region eu-west-1 \
#--function-name ingest_logging_data
#sleep 5


echo "  -> Deploy"
aws lambda update-function-code \
--region eu-west-1 \
--function-name ingest_logging_data  \
--zip-file fileb:///Users/adamalloul/lambda_api/ingest_logging_data.zip 
#--role arn:aws:iam::590746499688:role/shippy-logging-dev \
#--handler ingest_logging_data.ingest_logging_data \
#--runtime python2.7 \
#--profile adminuser
echo "  -> Wait 5s"
sleep 5
#
# echo "  -> Test"
# ./test_api_gateway.py


#echo "2- Create the new offers package"
#zip -q9 ingest_newOffers.zip ingest_newOffers.py
#zip -qr9 ingest_newOffers.zip elasticsearch
#zip -qr9 ingest_newOffers.zip urllib3
#zip -qr9 ingest_newOffers.zip requests_aws4auth
#zip -qr9 ingest_newOffers.zip requests
#zip -qr9 ingest_newOffers.zip requests-2.13.0.dist-info
#zip -qr9 ingest_newOffers.zip requests_aws4auth-0.9.dist-info
#zip -qr9 ingest_newOffers.zip urllib3-1.20.dist-info
#
## echo "  -> Delete function"
## aws lambda delete-function --function-name ingest_newOffers --region eu-west-1
## sleep 5
#
#echo "  -> Deploy"
#aws lambda update-function-code \
#--region eu-west-1 \
#--function-name ingest_newOffers  \
#--zip-file fileb:///Users/adamalloul/lambda_api/ingest_newOffers.zip
#--role arn:aws:iam::590746499688:role/shippy-offers-dev \
#--handler ingest_newOffers.ingest_newOffers \
#--runtime python2.7 \
#--profile adminuser
#sleep 5

#echo "3- Create the search offers package"
#zip -q9 search_offer.zip search_offer.py
#zip -qr9 search_offer.zip elasticsearch
#zip -qr9 search_offer.zip urllib3
#zip -qr9 search_offer.zip requests_aws4auth
#zip -qr9 search_offer.zip requests
#zip -qr9 search_offer.zip requests-2.13.0.dist-info
#zip -qr9 search_offer.zip requests_aws4auth-0.9.dist-info
#zip -qr9 search_offer.zip urllib3-1.20.dist-info
#
#echo "  -> Deploy"
#aws lambda update-function-code \
#--region eu-west-1 \
#--function-name search_offer \
#--zip-file fileb:///Users/adamalloul/lambda_api/search_offer.zip
# # --role arn:aws:iam::590746499688:role/shippy-search-offer-dev \
# # --handler search_offer.search_offer \
# # --runtime python2.7 \
# # --profile adminuser
#
#
# # sleep 5
# echo "  -> Test"
# # ./test_api_gateway.py
#
# echo "              Done"
# echo "==================================="
