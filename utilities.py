import boto3


def create_s3_session(access_key, secret_key):
    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key, )
    s3 = session.resource('s3')
    return s3
