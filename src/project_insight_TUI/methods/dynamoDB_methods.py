import boto3
import os
from dotenv import load_dotenv
from ..methods.initialize_methods import get_env_variables

load_dotenv()

def add_item_to_dynamodb(participant_id, study_start_date, study_end_date, phone_number, schedule_type, lb_link):

    env_vars = get_env_variables()

    Session = boto3.Session(
        aws_access_key_id=env_vars['aws_access_key_id'],
        aws_secret_access_key=env_vars['aws_secret_access_key'],
        region_name=env_vars['region']
    )

    dynamodb = Session.resource('dynamodb')
    table = dynamodb.Table(env_vars['table_name'])

    table.put_item(Item={
        "participant_id": participant_id,
        "study_start_date": study_start_date,
        "study_end_date": study_end_date,
        "phone_number": phone_number,
        "schedule_type": schedule_type,
        "lb_link": lb_link
    })

def get_item_from_dynamodb(participant_id):
    region = "us-east-1"

    # Get the AWS credentials from environment variables
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region = os.getenv('region', region)  # Use the provided region or default to us-east-1
    table_name = os.getenv('table_name')  # Use the provided table name or default to the one passed

    Session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    dynamodb = Session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    response = table.get_item(Key={"participant_id": participant_id})
    return response.get("Item", None)

def update_item_in_dynamodb(participant_id, update_field, new_value):
    region = "us-east-1"

    # Get the AWS credentials from environment variables
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region = os.getenv('region', region)  # Use the provided region or default to us-east-1
    table_name = os.getenv('table_name')  # Use the provided table name or default to the one passed

    Session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    dynamodb = Session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Implement logic to update item in DynamoDB
    table.update_item(
        Key={"participant_id": participant_id},
        UpdateExpression=f"SET {update_field} = :val",
        ExpressionAttributeValues={
            ":val": new_value  # Replace with the actual value to update
        }
    )

def delete_item_from_dynamodb(participant_id):
    region = "us-east-1"

    # Get the AWS credentials from environment variables
    aws_access_key_id = os.getenv('aws_access_key_id')
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    region = os.getenv('region', region)  # Use the provided region or default to us-east-1
    table_name = os.getenv('table_name')  # Use the provided table name or default to the one passed

    Session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    dynamodb = Session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Implement logic to delete item from DynamoDB
    table.delete_item(Key={"participant_id": participant_id})