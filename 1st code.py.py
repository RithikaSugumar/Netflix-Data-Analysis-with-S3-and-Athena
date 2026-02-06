import csv
import io

# Connect to S3
s3 = boto3.client('s3')

# Your bucket name and file key
BUCKET_NAME = "netflix-data-bucket"
FILE_KEY = "netflix_titles.csv"

def lambda_handler(event, context):
    # Get the CSV object from S3
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    
    # Read the file content
    data = obj['Body'].read().decode('utf-8')
    
    # Convert CSV string to dictionary rows
    reader = csv.DictReader(io.StringIO(data))
    
    # Example: count rows
    total_rows = sum(1 for row in reader)
    
    return {
        "statusCode": 200,
        "message": f"CSV read successfully from S3, total rows: {total_rows}"
    }
this code didn't work
