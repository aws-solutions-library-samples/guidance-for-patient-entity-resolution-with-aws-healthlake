import json
import sys
import time
import os
import boto3
from botocore.exceptions import ClientError

print('Loading function')
client = boto3.client('athena')
s3 = boto3.resource('s3')

def athena_start_query(query, database, s3OutputLocation):
    
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            
        },
        #WorkGroup='primary'  #can use either workgroup name or s3 location. going to use s3 location for now
        ResultConfiguration={
            'OutputLocation': s3OutputLocation
        }
        
    )
    
    print(response)
    
    return response
 
    
def has_query_succeeded(execution_id):
   
    isQueryStillRunning = True
    while (isQueryStillRunning) :
        print(isQueryStillRunning)
        
        getQueryExecutionResponse = client.get_query_execution(QueryExecutionId=execution_id)
        queryState = getQueryExecutionResponse["QueryExecution"]["Status"]["State"]
        if (queryState == "FAILED") :
            print("Failed")
            raise  Exception("The Amazon Athena query failed to run with error message: ", getQueryExecutionResponse["QueryExecution"]["Status"]["StateChangeReason"])
        elif  (queryState == "CANCELLED") :
            print("cancelled")
            raise  Exception("The Amazon Athena query was cancelled.")
        elif (queryState == "SUCCEEDED") :
            print("succeded")
            isQueryStillRunning = False
            copy_result_file(execution_id)
        else :
            print("waiting")
            # Sleep an amount of time in seconds before retrying again.
            time.sleep(3)
            
        print("The current status is: " + queryState)

def copy_result_file(execution_id):
    bucket = os.environ['S3_LOCATION_NAME']
    file_name = 'aer-input/healthlake-patients.csv'

    copy_source = {
        'Bucket': bucket,
        'Key': execution_id + '.csv'
    }
    s3.meta.client.copy(copy_source, bucket, file_name) 
    
def get_query_results(execution_id):
    response = client.get_query_results(
        QueryExecutionId=execution_id
    )

    results = response['ResultSet']['Rows']
    print(results)
    return results


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    
    
    query = 'SELECT id as source_id, name[1]."family" as last_name, name[1]."given"[1] as first_name, gender as gender, birthdate as birth_date, telecom[1]."value" as phone_nbr, address[1]."line"[1] as address1, address[1]."city" as city, address[1]."state" as state_code, address[1]."postalCode" as zip_code, address[1]."country" as country FROM patient'
    database = os.environ['DATABASE_NAME']
    print(database)
    s3OutputLocation = 's3://' + os.environ['S3_LOCATION_NAME']
    print(s3OutputLocation)
    try:
        response = athena_start_query(query, database, s3OutputLocation)
        queryExecutionId = response.get('QueryExecutionId')
        print(queryExecutionId)
        
        has_query_succeeded(queryExecutionId)
        
        #get_query_results(queryExecutionId)
        
        return True
        
        
    except ClientError as e:
        print('Error running athena query.')
        print("Unexpected error: %s" % e)
        return False
        
    except Exception as ex:
        print('Error running athena query.')
        print("Unexpected error: %s" % ex)
        return False
