import pulsar
import json
import os
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request
app = Flask(__name__)


 # Get properties to connect to Pulsar
service_url = os.environ.get('service_url')
token = os.environ.get('token')
namespace = os.environ.get('namespace')

@app.route('/write', methods=['POST'])
def writemessage():
    
    client = pulsar.Client(service_url,
                           authentication=pulsar.AuthenticationToken(token))

    producer = client.create_producer(f"{namespace}/raw")

    content = json.dumps(request.get_json())
    producer.send(bytes(content, 'utf-8'))
    
    client.close()

    return "{'outcome': 'SUCCESS'}"


if __name__ == '__main__':
    app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0')    
