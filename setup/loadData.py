print("Importing Modules")
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.query import SimpleStatement
from datasets import load_dataset
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

 # keys and tokens here
cass_user = os.environ.get('cass_user')
cass_pw = os.environ.get('cass_pw')
scb_path =os.environ.get('scb_path')
open_api_key= os.environ.get('openai_api_key')
openai.api_key = open_api_key
keyspace = os.environ.get('keyspace')

model_id = "text-embedding-ada-002"
EMBEDDING_MODEL = model_id
EMBEDDING_CTX_LENGTH = 8191
EMBEDDING_ENCODING = 'cl100k_base'

print("Configuring DB connection")
cloud_config= {
  'secure_connect_bundle': scb_path
}
auth_provider = PlainTextAuthProvider(cass_user, cass_pw)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace(keyspace)

print("Reset Schema")
# only use this to reset the schema
session.execute(f"""DROP INDEX IF EXISTS {keyspace}.text_title_embedding_indx""")
session.execute(f"""DROP TABLE IF EXISTS {keyspace}.news""")

print("Create Schema")
# # Create Table
session.execute(f"""CREATE TABLE IF NOT EXISTS {keyspace}.news
(publish_at date,
 id text,
 content text,
 title text,
 url text,
 hyperpartisan boolean,
 embedding vector<float, 1536>,
 PRIMARY KEY (publish_at, id))""")

# # Create Index
session.execute(f"""CREATE CUSTOM INDEX IF NOT EXISTS text_title_embedding_indx ON {keyspace}.news (embedding) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'""")

print("Load Data")
data = load_dataset('hyperpartisan_news_detection', 'byarticle', split='train')

data = data.to_pandas()
data.head()

data.drop_duplicates(subset='title', keep='first', inplace=True)
data.head()



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

print("Create data into Vector DB")
counter = 0
total = 10

for id, row in data.iterrows():

  # Create Embedding for each conversation row, save them to the database
  full_chunk = f"{row.text}"
  embedding = len_safe_get_embedding(full_chunk, average=True)

  published_at = row.published_at
  if(published_at == ""):
    published_at = "2017-01-01"

  query = SimpleStatement(
              f"""
              INSERT INTO {keyspace}.news
              (publish_at, id, content, title, url, hyperpartisan,embedding)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
               """
           )
  
  id = str(total).zfill(4)

  session.execute(query, (published_at,  id, row.text, row.title, row.url, row.hyperpartisan, embedding))

  # With free trial of openAI, the rate limit is set as 60/per min.  Please set this counter depends on your own rate limit.
  counter += 1

  print(id)
  total += 1
   
  # With OpenAPI free trial,  the rate limit is 60 / per min.   So when hit 60 requests,  the program will sleep for 60 seconds.
  if (counter >= 60):
    counter = 0;
    time.sleep(65)


