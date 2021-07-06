from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import os
from google.cloud import pubsub_v1
import pandas as pd
import logging
from time import sleep
from concurrent import futures

class PublishTopic:
    def __init__(self):
    	credentials_path = '/python-docker/Egen-privatekey.json'
    	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    	self.project_id = "egen-1625577545232"
    	self.topic_id = "crypto-live"
    	self.publisher = pubsub_v1.PublisherClient()
    	self.topic_path = 'projects/egen-1625577545232/topics/crypto-live'
    	self.publish_futures = []

    def getMatchData(self):
        url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest/symbol=BTC,ETH"
        parameters = {
          'start':'1',
          'limit':'15',
          'convert':'USD'
        }
        headers = {
          'Accepts': 'application/json',
          'X-CMC_PRO_API_KEY': "",
        }
        session = Session()
        session.headers.update(headers)

        try:
          response = session.get(url, params=parameters, stream=True)
          return (response.text)
        
        except (ConnectionError, Timeout, TooManyRedirects) as e:
          print(e)

    def get_callback(self, publish_future, data):
        def callback(publish_future):
            try:
                # Wait 60 seconds for the publish call to succeed.
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error("Publishing data timed out.")

        return callback

    def pushToTopic(self,data):
        # When you publish a message, the client returns a future.
        publish_future = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(self.get_callback(publish_future, data))
        self.publish_futures.append(publish_future)

        # Wait for all the publish futures to resolve before exiting.
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info("Published message to Topic")

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO)
    
    serv = PublishTopic()
    for i in range(60):
        message=serv.getMatchData()
        serv.pushToTopic(message)