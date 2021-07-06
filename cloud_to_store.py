import base64
import logging
import pandas as pd
import json
from google.cloud.storage import Client


class LoadToStorage:
    def __init__(self,event,context):
        self.event=event
        self.context=context
        self.bucket_name='egen-live-crypto-bucket'

    def getMsgData(self) -> str:
        logging.info("Function triggered, retrieving data")
        if data in self.event:
            message_chunk=base64.b64decode(self.event['Data']).decode('utf-8')
            logging.info("Datapoint validated")
            return message_chunk
        else:
            logging.error("No data found")

    def payloadToDf(self,message:str) -> pd.DataFrame:
        try:
            df=pd.DataFrame(json.loads(message))
            if not df.empty:
                logging.info("DF created")
            else:
                logging.info("Empty DF created")
        except Exception as e:
            logging.error(f"Error creating DF {str(e)}")
            raise

    def uploadToBucket(self,df,filename):
        storage_client=Client()
        bucket=storage_client.bucket(self.bucket_name)
        blob=bucket.blob(f"{filename}.csv")
        blob.upload_from_string(data=df.to_csv(index=False),content_type='text/csv')
        logging.info("File uploaded to bucket")




def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    logging.basicConfig(level=logging.INFO)
    service=LoadToStorage(event,context)
    message=service.getMsgData()
    df=service.payloadToDf(message)
    timestamp=str(int(time.time()))
    service.uploadToBucket(df,"egen-crypto-live"+timestamp)