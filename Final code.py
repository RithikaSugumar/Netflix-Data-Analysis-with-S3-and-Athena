import csv
import io
import json

# AWS resources
STREAM_NAME = "NetflixStream"
BUCKET_NAME = "netflix.s3"            
FILE_KEY = "netflix_titles.csv"          

# AWS clients
kinesis = boto3.client("kinesis", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    try:
        # Get the CSV file from S3
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
        data = obj['Body'].read().decode('utf-8')

        # Read CSV into a dictionary
        reader = csv.DictReader(io.StringIO(data))

        total_sent = 0
        for row in reader:
            # Clean data: strip extra spaces
            clean_data = json.dumps({k: v.strip() if v else "" for k, v in row.items()})

            try:
                # Send row to Kinesis
                response = kinesis.put_record(
                    StreamName=STREAM_NAME,
                    Data=clean_data,
                    PartitionKey=row.get("show_id", "default")
                )
                total_sent += 1
                print("Sent row:", response["SequenceNumber"])
            except Exception as e:
                print("Failed to send row:", e)

        return {
            "statusCode": 200,
            "message": f"CSV processed and sent to Kinesis successfully. Total rows sent: {total_sent}"
        }

    except s3.exceptions.NoSuchKey:
        return {
            "statusCode": 404,
            "message": f"File '{FILE_KEY}' not found in bucket '{BUCKET_NAME}'"
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "message": f"Error processing CSV: {str(e)}"
        }
