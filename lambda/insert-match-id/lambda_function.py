import boto3
import os
import requests
import csv
from requests_auth_aws_sigv4 import AWSSigV4


def putMatchId(resource_id, match_id, data_store_endpoint, region):
    # Frame the resource endpoint
    resource_path = "Patient/"+resource_id
    resource_endpoint = data_store_endpoint+resource_path
    
    session = boto3.session.Session(region_name=region)
    #client = session.client("healthlake")
  
    # Frame authorization
    auth = AWSSigV4("healthlake", session=session)

    # Calling data store FHIR endpoint using SigV4 auth
    r = requests.get(resource_endpoint, auth=auth, )

    onePatientDict = r.json()
    #print (str(onePatientDict))

    for item in onePatientDict["identifier"]:
       #print(str(item) + "\n")
       found = False
       if "assigner" in item and item["assigner"]["display"] == "AWS Entity Resolution":
          # find an item for AER match
          item["value"] = match_id
          found = True
       
    if not found: # need to append a new identifier
      # identifier object
      # based on: https://www.hl7.org/fhir/datatypes.html#Identifier
      id_dir = {
        'use': 'usual',
        'type': {
          'coding': [
            {
              'system': 'http://terminology.hl7.org/CodeSystem/v2-0203',
              'code': 'MR'
            }
          ]
        },
        'value': match_id,
        'assigner': {
          'display': 'AWS Entity Resolution'
        }
      }

      onePatientDict["identifier"].append(id_dir)
    
    print("found: " + str(found))
    print("onePatientDict: " + str(onePatientDict))

    result = requests.request('PUT', resource_endpoint, auth=auth, json=onePatientDict)
    print(result)


def lambda_handler(event, context):

    # Set the input arguments
    print("Event:" + str(event))
    
    bucket_name = os.environ['OUTPUT_BUCKET']
    workflow = os.environ['WORKFLOW']
    job_id = event['run_workflow']['Payload']['jobId']
    data_store_id = os.environ['DATASTORE_ID']
    region = os.environ['REGION']
    min_confidence = os.environ['CONFIDENCE_LEVEL']
    min_confidence = float(min_confidence)
    
    print(bucket_name)
    print(workflow)
    print(job_id)
    print(data_store_id)
    print(region)
    print("Confidence Level:" + str(min_confidence))
    
    prefix = workflow + '/' + job_id + '/success/'
    data_store_endpoint = 'https://healthlake.' + region + '.amazonaws.com/datastore/' + data_store_id + '/r4/'
    print(prefix)
    print(data_store_endpoint)
    
    results = []

    # read csv file from s3
    s3 = boto3.client(
        's3',
        region_name=region
    )
    
    # get the file name
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    key = response['Contents'][0]['Key']
    
    print(key)
    
    obj = s3.get_object(Bucket=bucket_name, Key=key) 
    data = obj['Body'].read().decode('utf-8').splitlines()
    
    records = csv.reader(data)
    headers = next(records)
    print('headers: %s' % (headers)) 
    
    try: 
      confidence_level = headers.index('ConfidenceLevel')
    except ValueError as e:
        print (e)
        return {
          "results": "No matches identified."
          }
          
    source_id = headers.index('source_id')
    match_id = headers.index('MatchID')
    print (str(confidence_level))
    print (str(source_id))
    print (str(match_id))
    
    count = 1
    for eachRecord in records:
        confidence = eachRecord[confidence_level]
        if len(confidence) > 0 and float(confidence) >= min_confidence:
          resource_id = eachRecord[source_id]
          matchId = eachRecord[match_id]
    
          print ("count: " + str(count))
          print("confidence: " + confidence)
          print ("source id: " + resource_id)
          print("match id: " + matchId + "\n")
    
          putMatchId(resource_id, matchId, data_store_endpoint, region)
          
          item = {"source_id": resource_id, "match_id": matchId}
          results.append(item)
    
        count = count + 1    
        
    # copy the file to latest result folder
    file_name = 'aer-latest-output/entity-resolution-results.csv'
    copy_source = {
      'Bucket': bucket_name,
      'Key': key
    }
    s3.copy(copy_source, bucket_name, file_name)
    
    return {
        "results": results
    }






