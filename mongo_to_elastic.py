#!/usr/bin/python
# -*- coding: utf-8 -*-
# import pandas as pd
# import pymongo
# from pymongo import MongoClient
# from elasticsearch import Elasticsearch
# from elasticsearch import helpers
# import json
# import psycopg2
# import datetime
# import os

# # Connect to MongoDB

# client = pymongo.MongoClient('mongodb://localhost')  # MongoClient(os.environ["localhost"])
# info = client.server_info()

# # print(info)

# db = client['testsmartrecon']
# collection = db['gl_balance']

# # print(db)
# # print(collection)

# elk_host = 'localhost'
# elk_port = '9200'
# elk_dbname_ES = 'gl_balance'  # index
# elk_user = 'elastic'
# elk_pw = 'NptWhopPgYXUciOtV7e8'
# elk_client_ES = Elasticsearch([elk_host], http_auth=(elk_user, elk_pw),
#                               port=elk_port, purge_unknown=True,
#                               applyAcls=False)


# # print(elk_client_ES)

# def migrate():
#     res = collection.find()
#     num_docs = 2
#     print (num_docs)
#     actions = []
#     for i in range(num_docs):
#         doc = res[i]
#         print (doc)
#         mongo_id = doc['_id']
#         doc.pop('_id', None)
#         actions.append({'_index': elk_dbname_ES, '_id': mongo_id,
#                        '_source': json.dumps(doc)})
#     helpers.bulk(elk_client_ES, actions)
#     json.dumps(doc, default=defaultconverter)
#     def defaultconverter(o):
#         if isinstance(o, datetime):
#             return o.__str__()
# if __name__ == "__main__":
#     migrate()


# # Read data from MongoDB and convert to Pandas dataframe
# data = list(collection.find())
# df = pd.DataFrame(data)

# # Connect to PostgreSQL
# conn = psycopg2.connect(
#     host="localhost",
#     database="testsmartrecon",
# )
#     # user="user",
#     # password="password"

# # Connect to Elasticsearch
# es = Elasticsearch([{"host": "localhost", "port": 9200}])

# # Index the data in Elasticsearch
# for i, doc in enumerate(data):
#     es.index(index="testindex", doc_type="testtype", id=i, body=doc)

# # Verify the indexed data in Elasticsearch
# res = es.search(index="testindex", body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total']['value'])
# for hit in res['hits']['hits']:
#     print("%(column1)s %(column2)s %(column3)s" % hit["_source"])


# # Create table in PostgreSQL
# # cursor = conn.cursor()
# # cursor.execute("""
# #     CREATE TABLE testtable (
# #         column1 text,
# #         column2 text,
# #         column3 text
# #     );
# # """)
# # conn.commit()

# # Write data to PostgreSQL
# # for index, row in df.iterrows():
# #     cursor.execute("""
# #         INSERT INTO testtable (column1, column2, column3)
# #         VALUES (%s, %s, %s);
# #     """, (row[0], row[1], row[2]))
# # conn.commit()

# # Close connections
# # cursor.close()
# # conn.close()
import pandas as pd
import pymongo
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import psycopg2
from datetime import datetime
# import datetime
import os

# Connect to MongoDB

# MongoClient(os.environ["localhost"])


def get_mongo_conn():
    client = pymongo.MongoClient('mongodb://localhost')
    info = client.server_info()
    db = client['testsmartrecon']
    collection = db['feeddata']
    return (collection)


def get_elk_conn():
    elk_host = "localhost"
    elk_port = "9200"
    index_settings = {"index.mapping.total_fields.limit": 2000}
    elk_dbname_ES = "feed_data"  # index
    elk_user = "elastic"
    elk_pw = "NptWhopPgYXUciOtV7e8"
    elk_conn = Elasticsearch([elk_host], body=index_settings, http_auth=(
        elk_user, elk_pw), port=elk_port, purge_unknown=True, applyAcls=False, readtimeout=100)
    return (elk_conn, elk_dbname_ES)

# Storing credentials securely
# elk_user = os.environ.get('ELK_USER')
# elk_pw = os.environ.get('ELK_PW')


def defaultconverter(o):
    if isinstance(o, datetime):
        return o.__str__()

# res = collection.find()
# num_docs = collection.count()
# actions = []
# for doc in res:
#     # print (doc)
#     mongo_id = str(doc['_id'])
#     # mongo_id = str(mo_id)
#     doc.pop('_id', None)
#     actions.append({'_index': elk_dbname_ES, '_id': mongo_id,
#                     '_source': json.dumps(doc, default=defaultconverter)})
# helpers.bulk(elk_client_ES, actions)
# print('inserted')


# def gl_balance():
#     res = collection.find()
#     num_docs = collection.count()
#     print(f"Number of documents: {num_docs}")
#     actions = []
#     for doc in res:
#         print (doc)
#         mongo_id = str(doc['_id'])
#         try:
#             doc['CLOSING BAL'] = str(float(doc['CLOSING BAL']))
#             doc['OPENING BAL'] = str(float(doc['OPENING BAL']))
#         except ValueError:
#             continue
#         # mongo_id = str(mo_id)
#         doc.pop('_id', None)
#         actions.append({'_index': elk_dbname_ES, '_id': mongo_id,
#                        '_source': json.dumps(doc,default=defaultconverter)})
#     helpers.bulk(elk_client_ES, actions)
    # json.dumps(doc, )

# def flows():
#     coll = get_mongo_conn()
#     elk_client, elk_db = get_elk_conn()
#     res = coll.find()
#     num_docs = coll.count()
#     print(f"Number of documents: {num_docs}")
#     actions = []
#     for doc in res:
#         # print (doc)
#         mongo_id = str(doc['_id'])
#         # mongo_id = str(mo_id)
#         doc.pop('_id', None)
#         actions.append({'_index': elk_db, '_id': mongo_id,
#                         '_source': json.dumps(doc,default=defaultconverter)})
#     helpers.bulk(elk_client, actions)


def sourceReference():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
        try:
            helpers.bulk(elk_client, batch_actions)
        except helpers.BulkIndexError as e:
            errors = e.errors
        print(errors)

        # print("Failed to index %d documents:" % len(e.errors))
        # for error in e.errors:
        #     print(error)
    # helpers.bulk(elk_client, actions)


# def recons():
#     res = collection.find()
#     num_docs = collection.count()
#     print(f"Number of documents: {num_docs}")
#     actions = []
#     for doc in res:
#         print (doc)
#         mongo_id = str(doc['_id'])
#         # mongo_id = str(mo_id)
#         doc.pop('_id', None)
#         actions.append({'_index': elk_dbname_ES, '_id': mongo_id,
#                        '_source': json.dumps(doc,default=defaultconverter)})
#     helpers.bulk(elk_client_ES, actions)

def recon_execution_details_log():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
        try:
            helpers.bulk(elk_client, batch_actions)
        except helpers.BulkIndexError as e:
            errors = e.errors
        print(errors)

        # write errors to text file
        # with open('\indexing_errors.txt', 'a') as f:
        #     for error in errors:
        #         f.write(json.dumps(error) + '\n')
        # continue
    # else:
    #     print(f"Indexed {num_docs} documents to Elasticsearch.")


def custom_reports():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
        try:
            helpers.bulk(elk_client, batch_actions)
        except helpers.BulkIndexError as e:
            errors = e.errors
        #     for i, item in enumerate(errors):
        #         with open('error_log.txt', 'a') as f:
        #             f.write(f"Failed to index document {item['index']['_id']}: {item['index']['error']}\n")
        #     print(f"Processed {len(actions) - len(errors)} documents successfully.")
        # else:
        #     print(f"Processed {len(actions)} documents successfully.")


def feeddata():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
        try:
            helpers.bulk(elk_client, batch_actions)
        except helpers.BulkIndexError as e:
            errors = e.errors
            print(errors)
        #     for i, item in enumerate(errors):
        #         with open('error_log.txt', 'a') as f:
        #             f.write(f"Failed to index document {item['index']['_id']}: {item['index']['error']}\n")
        #     print(f"Processed {len(actions) - len(errors)} documents successfully.")
        # else:
        #     print(f"Processed {len(actions)} documents successfully.")


def master_data():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
        try:
            helpers.bulk(elk_client, batch_actions)
        except helpers.BulkIndexError as e:
            errors = e.errors
            print(errors)


def matchRuleReference():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
        try:
            helpers.bulk(elk_client, batch_actions)
        except helpers.BulkIndexError as e:
            errors = e.errors
            print(errors)


def matchRuleSumColumns():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
        # helpers.bulk(elk_client, actions)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
    try:
        helpers.bulk(elk_client, batch_actions)
    except helpers.BulkIndexError as e:
        errors = e.errors
        print(errors)

def reconMetaInfo():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
        # helpers.bulk(elk_client, actions)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
    try:
        helpers.bulk(elk_client, batch_actions)
    except helpers.BulkIndexError as e:
        errors = e.errors
        print(errors)

def recon_meta_info():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
        # helpers.bulk(elk_client, actions)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
    try:
        helpers.bulk(elk_client, batch_actions)
    except helpers.BulkIndexError as e:
        errors = e.errors
        print(errors)

def reconaudit():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
        # helpers.bulk(elk_client, actions)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
    try:
        helpers.bulk(elk_client, batch_actions)
    except helpers.BulkIndexError as e:
        errors = e.errors
        print(errors)

def source_data():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
        # helpers.bulk(elk_client, actions)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
    try:
        helpers.bulk(elk_client, batch_actions)
    except helpers.BulkIndexError as e:
        errors = e.errors
        print(errors)

def users():
    coll = get_mongo_conn()
    elk_client, elk_db = get_elk_conn()
    res = coll.find()
    num_docs = coll.count()
    print(num_docs)
    actions = []
    for doc in res:
        mongo_id = str(doc['_id'])
        doc.pop('_id', None)
        action = {'_index': elk_db, '_id': mongo_id,
                  '_source': json.dumps(doc, default=defaultconverter)}
        actions.append(action)
        # helpers.bulk(elk_client, actions)
    # print('arjun',action)
    batch_size = 10000
    for i in range(0, len(actions), batch_size):
        batch_actions = actions[i:i+batch_size]
    try:
        helpers.bulk(elk_client, batch_actions)
    except helpers.BulkIndexError as e:
        errors = e.errors
        print(errors)

        
if __name__ == "__main__":
    feeddata()
