from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
#from cassandra.query import dict_factory
from cassandra.query import SimpleStatement
from itertools import islice
import tiktoken

import openai
import numpy as np
import pandas as pd
import time
from tenacity import retry, wait_random_exponential, stop_after_attempt, retry_if_not_exception_type
import os

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request
app = Flask(__name__)

 # keys and tokens here
cass_user = os.environ.get('cass_user')
cass_pw = os.environ.get('cass_pw')
scb_path =os.environ.get('scb_path')
open_api_key= os.environ.get('openai_api_key')
keyspace = os.environ.get('keyspace')
table_name = os.environ.get('table')

model_id = "text-embedding-ada-002"
openai.api_key = open_api_key

model_id = "text-embedding-ada-002"
EMBEDDING_MODEL = model_id
EMBEDDING_CTX_LENGTH = 8191
EMBEDDING_ENCODING = 'cl100k_base'

cloud_config= {
    'secure_connect_bundle': scb_path
    }
auth_provider = PlainTextAuthProvider(cass_user, cass_pw)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace(keyspace)
print("Configured DB connection")


# let's make sure to not retry on an invalid request, because that is what we want to demonstrate
@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6), retry=retry_if_not_exception_type(openai.InvalidRequestError))
def get_embedding(text_or_tokens, model=EMBEDDING_MODEL):
    return openai.Embedding.create(input=text_or_tokens, model=model)["data"][0]["embedding"]


def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := tuple(islice(it, n))):
        yield batch

def chunked_tokens(text, encoding_name, chunk_length):
    encoding = tiktoken.get_encoding(encoding_name)
    tokens = encoding.encode(text)
    chunks_iterator = batched(tokens, chunk_length)
    yield from chunks_iterator

def len_safe_get_embedding(text, model=EMBEDDING_MODEL, max_tokens=EMBEDDING_CTX_LENGTH, encoding_name=EMBEDDING_ENCODING, average=True):
    chunk_embeddings = []
    chunk_lens = []
    for chunk in chunked_tokens(text, encoding_name=encoding_name, chunk_length=max_tokens):
        chunk_embeddings.append(get_embedding(chunk, model=model))
        chunk_lens.append(len(chunk))

    if average:
        chunk_embeddings = np.average(chunk_embeddings, axis=0, weights=chunk_lens)
        chunk_embeddings = chunk_embeddings / np.linalg.norm(chunk_embeddings)  # normalizes length to 1
        chunk_embeddings = chunk_embeddings.tolist()
    return chunk_embeddings

@app.route('/hyperpartisan', methods=['POST'])
def checkhyperpartisan():
    data = request.get_json()

    #customer_input = "A combative President Trump launched an urgent, last-ditch bid Wednesday to revive an Obamacare repeal effort that had been left for dead just 24 hours earlier, imploring Republicans to stay in Washington until the job is done and warning that failure would decimate the party and its agenda.I'm ready to act, Mr. Trump said at the White House. For seven years you've promised the American people that you would repeal Obamacare. People are hurting. Inaction is not an option. The urgent public plea marked a confrontational shift in tone for Mr. Trump, who had been lobbying senators mainly behind the scenes, and a renewed commitment to the effort. One day earlier,Mr. Trump said Republicans should force Democrats to own Obamacare by letting it collapse under its unsustainable weight.SEE ALSO: Trump hosting GOP senators at White House on Obamacare repeal Mr. Trump got personal at times, leaning into Sen. Dean Heller, a Nevada Republican who faces a tough re-election battle next year and is wary of backing the repeal-and-replace effort."
    customer_input = data['content']

    # Create embedding based on same model
    embedding = len_safe_get_embedding(customer_input, average=True)

    query = SimpleStatement(
        f"""
        SELECT *
        FROM {keyspace}.{table_name}
        ORDER BY embedding ANN OF {embedding} LIMIT 5;
        """
        )

    results = session.execute(query)
    top_5_results = results._current_rows

    trueCnt = 0
    for row in top_5_results:
        print(f"""{row.title} {row.hyperpartisan}\n""")
        if(row.hyperpartisan == True):
            trueCnt = trueCnt + 1

    #data['embedding'] = embedding

    if (trueCnt/5 > 0):
        data['hyperpartisan'] = "true"
    else: 
        data['hyperpartisan'] = "false"
        
    return data

if __name__ == '__main__':
    app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0')    
