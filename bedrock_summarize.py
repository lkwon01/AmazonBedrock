#import all needed packages
import os
from IPython.display import Audio
import boto3
import uuid
import time
import json
from jinja2 import Template

#transcribe an audio file
audio = Audio(filename="dialog.mp3")
display(audio)
s3_client = boto3.client('s3', region_name='us-west-2')
bucket_name = os.environ['BucketName']
file_name = 'dialog.mp3'
s3_client.upload_file(file_name, bucket_name, file_name)
transcribe_client = boto3.client('transcribe', region_name='us-west-2')
job_name = 'transcription-job-' + str(uuid.uuid4())
response = transcribe_client.start_transcription_job(
    TranscriptionJobName=job_name,
    Media={'MediaFileUri': f's3://{bucket_name}/{file_name}'},
    MediaFormat='mp3',
    LanguageCode='en-US',
    OutputBucketName=bucket_name,
    Settings={
        'ShowSpeakerLabels': True,
        'MaxSpeakerLabels': 2
    }
)
while True:
    status = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
    if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
        break
    time.sleep(2)
print(status['TranscriptionJob']['TranscriptionJobStatus'])
if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
    
    # Load the transcript from S3.
    transcript_key = f"{job_name}.json"
    transcript_obj = s3_client.get_object(Bucket=bucket_name, Key=transcript_key)
    transcript_text = transcript_obj['Body'].read().decode('utf-8')
    transcript_json = json.loads(transcript_text)
    
    output_text = ""
    current_speaker = None
    
    items = transcript_json['results']['items']
    
    for item in items:
        
        speaker_label = item.get('speaker_label', None)
        content = item['alternatives'][0]['content']
        
        # Start the line with the speaker label:
        if speaker_label is not None and speaker_label != current_speaker:
            current_speaker = speaker_label
            output_text += f"\n{current_speaker}: "
            
        # Add the speech content:
        if item['type'] == 'punctuation':
            output_text = output_text.rstrip()
            
        output_text += f"{content} "
        
    # Save the transcript to a text file
    with open(f'{job_name}.txt', 'w') as f:
        f.write(output_text)
#Use an LLM
        
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-west-2')
with open(f'{job_name}.txt', "r") as file:
    transcript = file.read()
%%writefile prompt_template.txt
I need to summarize a conversation. The transcript of the 
conversation is between the <data> XML like tags.

<data>
{{transcript}}
</data>

The summary must contain a one word sentiment analysis, and 
a list of issues, problems or causes of friction
during the conversation. The output must be provided in 
JSON format shown in the following example. 

Example output:
{
    "sentiment": <sentiment>,
    "issues": [
        {
            "topic": <topic>,
            "summary": <issue_summary>,
        }
    ]
}

Write the JSON output and nothing more.

Here is the JSON output:

with open('prompt_template.txt', "r") as file:
    template_string = file.read()
data = {
    'transcript' : transcript
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
                "maxTokenCount": 512,
                "temperature": 0,
                "topP": 0.9
            }
        }
    )
}

response = bedrock_runtime.invoke_model(**kwargs)
response_body = json.loads(response.get('body').read())
generation = response_body['results'][0]['outputText']

print(generation)


       
