import boto3
import os

def lambda_handler(event, context):
    
    print(boto3.__version__)
    
    #workflowName = event["workflow"]
    workflowName = os.environ['WORKFLOW']
    print(workflowName)
    
    client = boto3.client('entityresolution')
    
    response = client.start_matching_job(  
        workflowName=workflowName
    )
    
    job_id = response['jobId']

    print ("Job id: " + str(job_id))
    
    return {
        "jobId" : job_id
    }
    