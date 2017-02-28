ELASTIC_LIBRARY = 'elasticsearch-5.2.0.dist-info  urllib3-1.20.dist-info elasticsearch urllib3'



zip -9 ingest_lambda.zip ingest_data.py
zip -r9 ingest_lambda.zip elasticsearch-5.2.0.dist-info
zip -r9 ingest_lambda.zip elasticsearch-5.2.0.dist-info
zip -r9 ingest_lambda.zip urllib3-1.20.dist-info
zip -r9 ingest_lambda.zip elasticsearch
zip -r9 ingest_lambda.zip urllib3
