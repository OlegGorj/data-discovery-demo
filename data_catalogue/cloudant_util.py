from cloudant.client import Cloudant


def get_cloudant_client(cloudant_credentials):
    return Cloudant(cloudant_credentials['username'], cloudant_credentials['password'], url=cloudant_credentials['url'],connect=True)

