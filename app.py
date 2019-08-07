from flask import Flask, request, abort
import json
import atexit
import os
from data_catalogue.cloudant_util import get_cloudant_client
from data_store.ibm_cloud_storage import get_cos_client
from data_store.ibm_cloud_storage import get_schema

app = Flask(__name__)

port = int(os.getenv('PORT', 8000))

"""
For any table you need to be able to:
1. Get where it is, and the corresponding metadata necessary to get information
2. Operations we can support:
    - View columns
    - List all tables that can be accesses
"""

with open('credentials.json') as outfile:
    credentials = json.load(outfile)

API_KEY = credentials["api_key"]
data_catalogue_client = get_cloudant_client(credentials['cloudant'])
object_storage = get_cos_client(credentials["cloud_storage"])


@atexit.register
def shutdown():
    data_catalogue_client.disconnect()


@app.route('/catalogue/tables', methods=['GET'])
def get_list_of_tables():
    headers = request.headers
    auth = headers.get("api_key")
    if auth == API_KEY:
        catalogue = data_catalogue_client['data_catalogue']
        response = []
        for document in catalogue:
            response.append({"table_name": document['table_name'], "source": document['source'],
                             "description": document['description']})
        return json.dumps(response)
    else:
        raise abort(401)


@app.route('/table/columns/<table_name>', methods=['GET'])
def get_table(table_name):
    headers = request.headers
    auth = headers.get("api_key")
    if auth == API_KEY:
        source = find_table_source(table_name)
        if source == "ibm_cloud_storage":
            columns = get_schema('nalrle', 'Output/target_pop_nov.csv')
            return json.dumps(columns)
        else:
            return 'The table has not been found'
    else:
        raise abort(401)


def find_table_source(table_name):
    catalogue = data_catalogue_client['data_catalogue']
    for document in catalogue:
        if document['table_name'] == table_name:
            return document['source']
    return 'did not find the table'


app.run(host='0.0.0.0', port=port, debug = True)
