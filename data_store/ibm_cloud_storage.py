import ibm_boto3
from ibm_botocore.client import Config, ClientError
import pandas as pd
import io


def get_cos_client(credentials):

    cos = ibm_boto3.resource("s3",
                             ibm_api_key_id=credentials['apikey'],
                             ibm_service_instance_id=credentials['resource_instance_id'],
                             ibm_auth_endpoint='https://iam.ng.bluemix.net/oidc/token',
                             config=Config(signature_version="oauth"),
                             endpoint_url="https://s3.us-east.cloud-object-storage.appdomain.cloud",
                             )
    return cos


def get_schema(bucket_name, file_name):
    dataframe = get_pandas_data_frame(bucket_name, file_name, object_storage)
    return dataframe.columns.tolist()


def get_pandas_data_frame(bucket_name, filename, resource,char_delim=',',encoding='utf-8'):
    obj = resource.Object(bucket_name=bucket_name, key=filename).get()
    return pd.read_csv(io.BytesIO(obj['Body'].read()), delimiter=char_delim,encoding=encoding)
