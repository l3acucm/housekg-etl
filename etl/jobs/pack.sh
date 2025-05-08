#!/bin/bash
cd lambda/ingestion/layer
zip -r ../../../../../iac/artifacts/requests_layer.zip python
cd ..
zip -j ../../../../iac/artifacts/ingestion_lambda.zip main.py
cd ../../sagemaker
tar -czf ../../../iac/artifacts/model_training.tar.gz model_training.py