from pulsar import Function
import requests
import json
import os 

from dotenv import load_dotenv
load_dotenv()


class SplitFunction(Function):
  def __init__(self):
    self.hyperpartisan_topic = os.environ.get('hyperpartisan_topic')
    self.nonhyperpartisan_topic = os.environ.get('nonhyperpartisan_topic')


  def process(self, input, context):
    api_url = os.environ.get('api_url')
    
    jsonInput = json.loads(input)
    response = requests.post(api_url, json=jsonInput)
    result =  response.json()
    
    if result['hyperpartisan'] == "true":
      context.publish(self.hyperpartisan_topic, json.dumps(result))      
    else:
      context.publish(self.nonhyperpartisan_topic, json.dumps(result))

