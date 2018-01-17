import os
import json
import sys
import uuid
import datetime
import cStringIO
import webapp2

# Append directory containing libraries to system path
sys.path.append('lib')

import cloudstorage as gcs
from google.appengine.api import app_identity
from google.appengine.api import taskqueue
from httplib2 import Http
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build
from bigquery import get_client

# Retry can help overcome transient urlfetch or GCS issues, such as timeouts.
my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=15)

# All requests to GCS using the GCS client within current GAE request and
# current thread will use this retry params as default. If a default is not
# set via this mechanism, the library's built-in default will be used.
# Any GCS client function can also be given a more specific retry params
# that overrides the default.
gcs.set_default_retry_params(my_default_retry_params)

write_retry_params = gcs.RetryParams(backoff_factor=1.1)

# Project Globals
SERVICE_ACCOUNT_EMAIL = 'service-account@email.com'     # change to your service account email
project_id = 'projectId'                                # change to your project id
bq_dataset = 'dataset_test'                             # change to your bigquery dataset name
bq_table_schema_file = 'bq-table-schema.json'
private_key_file = 'privatekey.pem'

output = cStringIO.StringIO()


# Route = /collect
class DataCollectionHandler(webapp2.RequestHandler):
    def post(self):

        data = json.loads(self.request.body)

        accountid = data['accountId']
        dic = data['data']

        jsondata = json.dumps(dic).encode('utf8')

        # write data to string buffer
        global output
        output.write(jsondata + "\n")

        print "Wrote data to Log file.\n"

        if accountid is not None:

            print "---data for---" + accountid + "---\n"
            print jsondata

            f = file(private_key_file, 'rb')
            key = f.read()
            f.close()

            credentials = SignedJwtAssertionCredentials(
                SERVICE_ACCOUNT_EMAIL,
                key,
                scope='https://www.googleapis.com/auth/bigquery')

            # BigQuery project id as listed in the Google Developers Console.

            table = 'prefix_' + accountid

            client = get_client(project_id, credentials=credentials)

            # Check if a table exists.
            exists = client.check_table(bq_dataset, table)

            if exists is None or exists is False:
                rec_file = file(bq_table_schema_file, 'rb')
                rec = rec_file.read()
                f.close()

                schema = json.loads(rec, 'utf-8')
                client.create_table(bq_dataset, table, schema)

                print 'New Table ' + table + ' created!'

            self.response.out.write(jsondata)

            # Add the task to the default queue.
            taskqueue.add(url='/worker',
                          params={'json': jsondata, 'dataset': bq_dataset, 'table': table})

        print 'Data added to Task Queue!'


# Route = /worker
class TaskQueueWorker(webapp2.RequestHandler):
    def post(self):  # should run at most 1/s
        json_row = self.request.get('json')
        dataset_name = self.request.get('dataset')
        table_name = self.request.get('table')

        # OBTAIN THE KEY FROM THE GOOGLE APIs CONSOLE
        # More instructions here: http://goo.gl/w0YA0
        f = file(private_key_file, 'rb')
        key = f.read()
        f.close()

        credentials = SignedJwtAssertionCredentials(
            SERVICE_ACCOUNT_EMAIL,
            key,
            scope='https://www.googleapis.com/auth/bigquery')

        http = Http()
        http = credentials.authorize(http)

        bigquery = build('bigquery', 'v2', http=http)

        stream_row_to_bigquery(bigquery, project_id, dataset_name, table_name,
                               json.loads(json_row), num_retries=5)
        print 'inserted to bigquery table: ' + table_name + '\n'


# Route = /backup
class BackupToGCS(webapp2.RequestHandler):
    def get(self):
        bucket_name = os.environ.get('BUCKET_NAME',
                                     app_identity.get_default_gcs_bucket_name())

        lastHourDateTime = datetime.datetime.now() - datetime.timedelta(hours=1)

        lastHourLogFile = "log_" + lastHourDateTime.strftime("%Y%m%d-%H") + ".log"

        bucket = '/' + bucket_name
        lastHourLogFile = bucket + '/' + lastHourLogFile

        try:
            gcs_file = gcs.open(lastHourLogFile,
                                'w',
                                content_type='text/plain',
                                options={'x-goog-meta-foo': 'foo',
                                         'x-goog-meta-bar': 'bar'},
                                retry_params=write_retry_params)
            gcs_file.write(output.getvalue().encode('utf-8'))
            output.close()
            gcs_file.close()
        except Exception, e:
            print "file not found!"

        global output
        output = cStringIO.StringIO()

        print "Last hour log file written to GCS."


# Route = /
class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/html'
        self.response.write('<h1>Welcome! I\'m ready to rock & roll \o/</h1>')


# GLOBAL FUNCTIONS
# ----------------------------------------------------------------------------------------------------------------------

def stream_row_to_bigquery(bigquery, project_id, dataset_id, table_name, row, num_retries=5):
    # Generate a unique row id so retries
    # don't accidentally duplicate insert
    insert_all_data = {
        'insertId': str(uuid.uuid4()),
        "rows": [{"json": row}],
        "ignoreUnknownValues": True
    }

    return bigquery.tabledata().insertAll(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_name,
        body=insert_all_data).execute(num_retries=num_retries)


# APP ROUTES
app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ('/worker', TaskQueueWorker),
    ('/backup', BackupToGCS),
    ('/collect', DataCollectionHandler)
], debug=True)
