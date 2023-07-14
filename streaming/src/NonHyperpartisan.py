import pulsar,time
import json
import pulsar,time
import os
from dotenv import load_dotenv

load_dotenv()

 # Get properties to connect to Pulsar
service_url = os.environ.get('service_url')
token = os.environ.get('token')
namespace = os.environ.get('namespace')

client = pulsar.Client(service_url,
                        authentication=pulsar.AuthenticationToken(token))

consumer = client.subscribe(f"{namespace}/non-hyperpartisan", 'non-hyperpartisan-sub')
waitingForMsg = True
while waitingForMsg:
    try:
        print("receiving..")
        msg = consumer.receive()
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        # Acknowledging the message to remove from message backlog
        consumer.acknowledge(msg)
    except:
        print("Still waiting for a message...");
    
    time.sleep(5)
    

client.close()