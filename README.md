# Big Data Collection on GCP

### A Google Cloud Platform App
Works with Google App Engine Standard platform

### Get Started

Create an App Engine application on GCP

Enable billing

Prepare service account

Get private key (p12 format)

Convert P12 to PEM key

`cat privatekey.p12 | openssl pkcs12 -nodes -nocerts -passin pass:notasecret | openssl rsa > pk.pem`

Place privatekey.pem file in the root directory

Prepare app libraries and dependencies by running the command

`pip install -r requirements.txt -t lib`

Download and install GoogleAppEngineLauncher from [here](https://cloud.google.com/appengine/docs/standard/python/download#appengine_sdk)
 
Add application in GoogleAppEngineLauncher, configure ports

Browse

_You can test the application by using the test shell script file 
`go-crazy.sh` which makes post request to your GAE application as many 
times as specified in the argument_