#Import all needed packages
import boto3, os
from helpers.Lambda_Helper import Lambda_Helper
from helpers.S3_Helper import S3_Helper
from helpers.Display_Helper import Display_Helper
lambda_helper = Lambda_Helper()
# deploy_function
# add_lambda_trigger
s3_helper = S3_Helper()
# upload_file
# download_object 
# list_objects
display_helper = Display_Helper()
# text_file
# json_file
bucket_name_text = os.environ['LEARNERS3BUCKETNAMETEXT']
%%writefile prompt_template.txt
I need to summarize a conversation. The transcript of the conversation is between the <data> XML like tags.

<data>
{{transcript}}
</data>

The summary must contain a one word sentiment analysis, and a list of issues, problems or causes of friction
during the conversation. The output must be provided in JSON format shown in the following example. 

Example output:
{
    "version": 0.1,
    "sentiment": <sentiment>,
    "issues": [
        {
            "topic": <topic>,
            "summary": <issue_summary>,
        }
    ]
}

An `issue_summary` must only be one of:
{%- for topic in topics %}
 - `{{topic}}`
{% endfor %}

Write the JSON output and nothing more.

Here is the JSON output:
display_helper.text_file('prompt_template.txt')

#Create the Lambda function
%%writefile lambda_function.py


#############################################################
#
# This Lambda function is written to a file by the notebook 
# It does not run in the notebook!
#
#############################################################

import boto3
import json 
from jinja2 import Template

s3_client = boto3.client('s3')
bedrock_runtime = boto3.client('bedrock-runtime', 'us-west-2')

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # One of a few different checks to ensure we don't end up in a recursive loop.
    if "-transcript.json" not in key: 
        print("This demo only works with *-transcript.json.")
        return
    
    try: 
        file_content = ""
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        file_content = response['Body'].read().decode('utf-8')
        
        transcript = extract_transcript_from_textract(file_content)

        print(f"Successfully read file {key} from bucket {bucket}.")

        print(f"Transcript: {transcript}")
        
        summary = bedrock_summarisation(transcript)
        
        s3_client.put_object(
            Bucket=bucket,
            Key='results.txt',
            Body=summary,
            ContentType='text/plain'
        )
        
    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error occurred: {e}")
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f"Successfully summarized {key} from bucket {bucket}. Summary: {summary}")
    }
        
        
        
def extract_transcript_from_textract(file_content):

    transcript_json = json.loads(file_content)

    output_text = ""
    current_speaker = None

    items = transcript_json['results']['items']

    # Iterate through the content word by word:
    for item in items:
        speaker_label = item.get('speaker_label', None)
        content = item['alternatives'][0]['content']
        
        # Start the line with the speaker label:
        if speaker_label is not None and speaker_label != current_speaker:
            current_speaker = speaker_label
            output_text += f"\n{current_speaker}: "
        
        # Add the speech content:
        if item['type'] == 'punctuation':
            output_text = output_text.rstrip()  # Remove the last space
        
        output_text += f"{content} "
        
    return output_text
        

def bedrock_summarisation(transcript):
    
    with open('prompt_template.txt', "r") as file:
        template_string = file.read()

    data = {
        'transcript': transcript,
        'topics': ['charges', 'location', 'availability']
    }
    
    template = Template(template_string)
    prompt = template.render(data)
    
    print(prompt)
    
    kwargs = {
        "modelId": "amazon.titan-text-express-v1",
        "contentType": "application/json",
        "accept": "*/*",
        "body": json.dumps(
            {
                "inputText": prompt,
                "textGenerationConfig": {
                    "maxTokenCount": 2048,
                    "stopSequences": [],
                    "temperature": 0,
                    "topP": 0.9
                }
            }
        )
    }
    
    response = bedrock_runtime.invoke_model(**kwargs)

    summary = json.loads(response.get('body').read()).get('results')[0].get('outputText')    
    return summary
    
    
lambda_helper.deploy_function(
    ["lambda_function.py", "prompt_template.txt"],
    function_name="LambdaFunctionSummarize"
)
lambda_helper.filter_rules_suffix = "json"
lambda_helper.add_lambda_trigger(bucket_name_text)
# display_helper.json_file('demo-transcript.json')
s3_helper.upload_file(bucket_name_text, 'demo-transcript.json')

