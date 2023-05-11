import getpass
import oracledb
import sql_consts
import redis
import json
from datetime import datetime

# import variable sql_consts.py
from sql_consts import product_qualifier_query


def output_type_handler(cursor, name, default_type, size, precision, scale):
    if default_type == oracledb.DB_TYPE_CLOB:
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if default_type == oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    if default_type == oracledb.DB_TYPE_NCLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_NVARCHAR, arraysize=cursor.arraysize)


connection = oracledb.connect(
    user="apps", password="apps", dsn="{host}:1521/{service}"
)

connection.outputtypehandler = output_type_handler

r = redis.Redis(host="{redis_host}", port=6379, db=0)

print("Successfully connected to Oracle Database")

# print(sql_consts.product_qualifier_query)

try:
    cursor = connection.cursor()

    time_started = datetime.now()

    print(f"Time started to fetch data from Oracle: {time_started}")

    data_fetch = cursor.execute(sql_consts.product_qualifier_query).fetchall()

    print(f" Time taken to fetch data from Oracle: { datetime.now() - time_started}")

    pipeline = r.pipeline()

    time_started = datetime.now()

    print(f"Time started to push data to redis: {time_started}")

    # Now query the rows back
    for row in data_fetch:
        item = row[0]
        async_prod = row[1]
        async_comb = row[2]
        async_item_qf = row[3]

        # Pushing data to redis
        # print(f"Pushing data for the item : {item}")

        # pushing data with pipeline
        ## with pipeline it reduces network roundtrips

        if async_prod is not None:
            pipeline.set("ASYNC_PROD_" + item, json.dumps(async_prod))
        if async_comb is not None:
            pipeline.set("ASYNC_COMB_" + item, json.dumps(async_comb))
        if async_item_qf is not None:
            pipeline.set("ASYNC_ITEM_QF_" + item, json.dumps(async_item_qf))

        # pushing data without pipeline

        # if r.get("ASYNC_PROD_" + item) is None and async_prod is not None:
        #     r.set("ASYNC_PROD_" + item, json.dumps(async_prod))
        # else:
        #     print("ASYNC_PROD_ " + item + "Already pushed")
        # if r.get("ASYNC_COMB_" + item) is None and async_comb is not None:
        #     r.set("ASYNC_COMB_" + item, json.dumps(async_comb))
        # else:
        #     print("ASYNC_COMB_ " + item + "Already pushed")
        # if r.get("ASYNC_ITEM_QF_" + item) is None and async_item_qf is not None:
        #     r.set("ASYNC_ITEM_QF_" + item, json.dumps(async_item_qf))
        # else:
        #     print("ASYNC_ITEM_QF_ " + item + "Already pushed")

    resp = pipeline.execute()
    print(f" Time taken to fetch data to Redis: {datetime.now() - time_started }")
    print(str(len(resp)) + "Inserted into redis cache")
finally:
    connection.close()
    print("database connection closed")
    r.close()
    print("redis connection closed")
