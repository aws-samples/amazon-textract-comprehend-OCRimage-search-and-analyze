""" Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0. """

from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth
from requests_aws4auth import AWS4Auth
import base64
from s3transfer.manager import TransferManager
import os
import os.path
import sys
import boto3
import json
import io
from io import BytesIO
import sys
from trp import Document

try:
    from urllib.parse import unquote_plus
except ImportError:
     from urllib import unquote_plus


print('setting up boto3')

root = os.environ["LAMBDA_TASK_ROOT"]
sys.path.insert(0, root)
print(boto3.__version__)
print('core path setup')
s3 = boto3.resource('s3')
s3client = boto3.client('s3')

host= os.environ['esDomain']
print("ES DOMAIN IS..........")
region=os.environ['AWS_REGION']

service = 'es'
credentials = boto3.Session().get_credentials()

def connectES():
 print ('Connecting to the ES Endpoint {0}')
 awsauth = AWS4Auth(credentials.access_key, 
 credentials.secret_key, 
 region, service,
 session_token=credentials.token)
 try:
  es = Elasticsearch(
   hosts=[{'host': host, 'port': 443}],
   http_auth = awsauth,
   use_ssl=True,
   verify_certs=True,
   connection_class=RequestsHttpConnection)
  return es
 except Exception as E:
  print("Unable to connect to {0}")
  print(E)
  exit(3)
print("sucess seting up es")

print("setting up Textract")
# get the results
textract = boto3.client(
         service_name='textract',
         region_name=region)

print('initializing comprehend')
comprehend = boto3.client(service_name='comprehend', region_name=region)
print('done')

def outputForm(page):
        csvData = []
        for field in page.form.fields:
            csvItem  = []
            if(field.key):
                csvItem.append(field.key.text)
            else:
                csvItem.append("")
            if(field.value):
                csvItem.append(field.value.text)
            else:
                csvItem.append("")
            csvData.append(csvItem)
        return csvData

def outputTable(page):
    csvData = []
    print("//////////////////")
    #print(page)
    for table in page.tables:
            csvRow = []
            csvRow.append("Table")
            csvData.append(csvRow)
            for row in table.rows:
                csvRow  = []
                for cell in row.cells:
                    csvRow.append(cell.text)
                csvData.append(csvRow)
            csvData.append([])
            csvData.append([])
    return csvData
# --------------- Main Lambda Handler ------------------


def handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    print("key is"+key)
    print("bucket is"+bucket)
    text=""
    textvalues=[]
    textvalues_entity={}
    try:
        s3.Bucket(bucket).download_file(Key=key,Filename='/tmp/{}')
        # Read document content
        with open('/tmp/{}', 'rb') as document:
            imageBytes = bytearray(document.read())
        print("Object downloaded")
        response = textract.analyze_document(Document={'Bytes': imageBytes},FeatureTypes=["TABLES", "FORMS"])
        document = Document(response)
        table=[]
        forms=[]
        #print(document)
        for page in document.pages:
                table = outputTable(page)
                forms = outputForm(page)
        print(table)
        blocks=response['Blocks']
        for block in blocks:
            if block['BlockType'] == 'LINE':
                text += block['Text']+"\n"
        print(text)
        # Extracting Key Phrases
        keyphrase_response = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
        KeyPhraseList=keyphrase_response.get("KeyPhrases")
        for s in KeyPhraseList:
              textvalues.append(s.get("Text"))
                    
        detect_entity= comprehend.detect_entities(Text=text, LanguageCode='en')
        EntityList=detect_entity.get("Entities")
        for s in EntityList:
                textvalues_entity.update([(s.get("Type").strip('\t\n\r'),s.get("Text").strip('\t\n\r'))])

        s3url= 'https://s3.console.aws.amazon.com/s3/object/'+bucket+'/'+key+'?region='+region
        
        searchdata={'s3link':s3url,'KeyPhrases':textvalues,'Entity':textvalues_entity,'text':text, 'table':table, 'forms':forms}
        print(searchdata)
        print("connecting to ES")
        es=connectES()
        #es.index(index="resume-search", doc_type="_doc", body=searchdata)
        es.index(index="document", doc_type="_doc", body=searchdata)
        print("data uploaded to Elasticsearch")
        return 'keyphrases Successfully Uploaded'
    except Exception as e:
        print(e)
        print('Error: ')
        raise e
