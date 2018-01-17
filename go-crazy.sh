#!/bin/bash

 for i in $(eval echo {1..$1})
 do
  curl --request POST \
  --url https://big-data-collection.appspot.com/collect \
  --header 'content-type: application/json' \
  --data '{
	"accountId": "table_test",
	"data": {
		"sessionId": "39a08c7f-286c-0d0f-c191-d5e5bba6402c",
		"visitorId": "ce144781-3820-f16c-53d8-593b906b6886",
		"document": {
			"title": "Rahul Sharma | rahul.my",
			"url": {
				"host": "www.rahul.my",
				"hostname": "www.rahul.my",
				"pathname": "/",
				"protocol": "https:"
			}
		},
		"timestamp": "2018-01-18T19:00:47.556Z",
		"ip": "8.8.8.8"
	}
}'

done