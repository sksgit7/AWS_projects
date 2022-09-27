import json
import csv
import boto3
import time

#athena client
client = boto3.client('athena')

#S3 boto3 resource
s3 = boto3.resource('s3')

#output bucket
bucket = s3.Bucket('output-bucket-sks')

#output location
output_path = 'results.csv'

def athena_query():
    
    #start athena query execution
    response = client.start_query_execution(
        QueryString = '''
                        SELECT state_ut_wise, active_cases, total_cases, round((1.0*active_cases)*100/(1.0*total_cases),2) as pct_active_of_total 
                        FROM "test-sks"."crawler_bucket_sks" where (1.0*active_cases)*100/(1.0*total_cases) > 20
                        order by pct_active_of_total desc;
                        ''',
        QueryExecutionContext = {'Database' : 'test-sks'},
        ResultConfiguration = {'OutputLocation' : 's3://athena-results-sks'}
    )
    #get query execution id
    queryExecutionId = response['QueryExecutionId']
    
    #check every 1 second for query status for 5 seconds. If succeeded, fetch results
    counter = 5
    while counter>0:
		#get the status of the query
        queryStatus = client.get_query_execution(QueryExecutionId = queryExecutionId)['QueryExecution']['Status']['State']
        
		#if query succeeded we will send the queryExecutionId else 0
		if queryStatus == 'SUCCEEDED':
            return queryExecutionId
        elif queryStatus == 'FAILED':
            return 0
        time.sleep(1)
        
#Function to read the rows from query result and write to results.csv file in /tmp directory of Lambda
def result_file(results):
    with open('/tmp/results.csv', 'w', newline='') as f:
        w = csv.writer(f)
        for row in results['ResultSet']['Rows']:
            rowList = []
            for col in row['Data']:
                if col.get('VarCharValue'):
                    rowList.append(col.get('VarCharValue'))
                else:
                    rowList.append('')
            
            print(row)
            print(rowList)
            w.writerow(rowList)
            
    #upload file to the bucket
    bucket.upload_file('/tmp/results.csv', output_path)
        
def lambda_handler(event, context):
    # TODO implement
    queryExecutionId = athena_query()
	if queryExecutionId == 0:
		print('Query failed')
	else:
		#get query result
		results = client.get_query_results(QueryExecutionId = queryExecutionId)
		result_file(results)
