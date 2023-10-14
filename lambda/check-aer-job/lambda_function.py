import boto3
import os

def lambda_handler(event, context):
    
    print(boto3.__version__)
    
    job_id = event['run_workflow']['Payload']['jobId']
    workflow_name = os.environ['WORKFLOW']
    print ("job_id: " + job_id)
    print ("workflow: " + workflow_name)
    
    client = boto3.client('entityresolution')
    
    response = client.get_matching_job (
        jobId=job_id,
        workflowName=workflow_name
    )
    
    status = response['status']

    print ("status: " + str(status))
    
    return {
        "status": status
    }
    