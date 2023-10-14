import time
import os
import boto3
    
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    
    client = boto3.client('glue')
    crawler_name = os.environ['CRAWLER_NAME']
    print("Crawler: " + crawler_name)

    response = client.get_crawler(Name=crawler_name)
    print("Response: " + str(response))
    crawler = response['Crawler']
    print("Crawler: " + str(crawler))

    # Check last crawl
    last_crawl = False
    if 'LastCrawl' in crawler:
        status = crawler['LastCrawl']['Status']
        print ("Last crawl status: " + status)
        if status == 'SUCCEEDED':
            last_crawl = True
    
    # Only run crawler if there is no successful last crawl
    if not last_crawl:
        response = client.start_crawler(Name=crawler_name)

        is_running = True
        while (is_running):
            time.sleep(60) # Wait for 60 seconds

            response = client.get_crawler(Name=crawler_name)
            print("Response: " + str(response))
            status = response['Crawler']['State']
            print("State: " + status)

            if status == 'READY':
                is_running = False
                
    return {
        'status': status
    }
        
    

