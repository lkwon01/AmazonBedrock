#Import all needed packages
import boto3
import json
import os

bedrock = boto3.client('bedrock', region_name="us-west-2")
from helpers.CloudWatchHelper import CloudWatch_Helper
cloudwatch = CloudWatch_Helper()
log_group_name = '/my/amazon/bedrock/logs'
cloudwatch.create_log_group(log_group_name)
loggingConfig = {
    'cloudWatchConfig': {
        'logGroupName': log_group_name,
        'roleArn': os.environ['LOGGINGROLEARN'],
        'largeDataDeliveryS3Config': {
            'bucketName': os.environ['LOGGINGBUCKETNAME'],
            'keyPrefix': 'amazon_bedrock_large_data_delivery',
        }
    },
    's3Config': {
        'bucketName': os.environ['LOGGINGBUCKETNAME'],
        'keyPrefix': 'amazon_bedrock_logs',
    },
    'textDataDeliveryEnabled': True,
}
bedrock.put_model_invocation_logging_configuration(loggingConfig=loggingConfig)
bedrock.get_model_invocation_logging_configuration()
bedrock_runtime = boto3.client('bedrock-runtime', region_name="us-west-2")
prompt = "Write an article about the fictional planet Foobar."

kwargs = {
    "modelId": "amazon.titan-text-express-v1",
    "contentType": "application/json",
    "accept": "*/*",
    "body": json.dumps(
        {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 512,
                "temperature": 0.7,
                "topP": 0.9
            }
        }
    )
}

response = bedrock_runtime.invoke_model(**kwargs)
response_body = json.loads(response.get('body').read())

generation = response_body['results'][0]['outputText']

print(generation)
cloudwatch.print_recent_logs(log_group_name)