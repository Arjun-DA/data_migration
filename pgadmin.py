import psycopg2
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from psycopg2.extras import execute_values, execute_batch, Json
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch, helpers
from psycopg2 import sql
import json
import csv
from datetime import datetime
import datetime

# date_format = '%Y-%m-%dT%H:%M:%S'


def get_elk_conn():
    elk_host = "localhost"
    elk_port = "9200"
    # elk_dbname_ES = "testsmartrecon" #index
    elk_user = "elastic"
    elk_pw = "NptWhopPgYXUciOtV7e8"
    elk_conn = Elasticsearch([elk_host], http_auth=(
        elk_user, elk_pw), port=elk_port, purge_unknown=True, applyAcls=False)
    return elk_conn


def get_db_cursor():
    pgsql_host = "localhost"
    pgsql_port = "5432"
    pgsql_dbname = "recon"
    pgsql_user = "recon"
    pgsql_pw = "admin"
    pgsql_conn = psycopg2.connect(
        host=pgsql_host, port=pgsql_port, dbname=pgsql_dbname, user=pgsql_user, password=pgsql_pw)
    pgsql_cursor = pgsql_conn.cursor()
    return pgsql_conn, pgsql_cursor


def gl_summary():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'gl_balance' in indices:
        drop = f"DROP TABLE IF EXISTS gl_balance;"
        # print(indices)
        pgsql_db_cursor.execute(drop)
        # pgsql_db_conn.commit()
        result = elk_client_ES.search(index='gl_balance', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        sample = {}
        create_table_query = ''
        create_table_query = '''CREATE TABLE IF NOT EXISTS gl_balance (
                        ID VARCHAR(500) PRIMARY KEY,
                        "Account Number" VARCHAR(500),
                        reconId VARCHAR(500),
                        reconName VARCHAR(500),
                        reconProcess VARCHAR(500),
                        sourceName VARCHAR(500),
                        stmtDate DATE,
                        "CLOSING BAL" DOUBLE PRECISION,
                        ExecutionId VARCHAR(500),
                        "OPENING BAL" DOUBLE PRECISION,
                        "Account Name" VARCHAR(500)
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            # print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            # print(source_data)
            # val = [[val for val in project.values()] for project in source_data]
            # value_ty = [str(v) if v != '' else 'null' for v in val]
            # column = [[col for col in project.keys()] for project in source_data]
            # column = column[0]
            # columns = [item.strip().replace(" ", "") for item in column]
            # print(columns)
            for record in source_data:
                ID = str(Id)
                Account_Number = record['Account Number']
                reconId = record['reconId']
                reconName = record['reconName']
                reconProcess = record['reconProcess']
                sourceName = record['sourceName']
                stmtDate = record['stmtDate']
                CLOSING_BAL = float(record['CLOSING BAL'])
                ExecutionId = record['ExecutionId']
                OPENING_BAL = float(record['OPENING BAL'])
                Account_Name = record['Account Name']
            try:
                insert_query = """INSERT INTO gl_balance (ID,"Account Number", reconId, reconName, reconProcess, sourceName, stmtDate, "CLOSING BAL", ExecutionId, "OPENING BAL", "Account Name")
        VALUES (%s, %s, %s, %s, %s, %s, to_date(%s, 'DDMMYYYY'), %s, %s, %s, %s)"""
                values = (str(ID) if ID else None,
                          Account_Number if Account_Number else None,
                          reconId if reconId else None,
                          reconName if reconName else None,
                          reconProcess if reconProcess else None,
                          sourceName if sourceName else None,
                          stmtDate if stmtDate else None,
                          float(CLOSING_BAL) if CLOSING_BAL else None,
                          ExecutionId if ExecutionId else None,
                          float(OPENING_BAL) if OPENING_BAL else None,
                          Account_Name if Account_Name else None)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")
            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            try:
                pgsql_db_cursor.execute("SELECT * FROM gl_balance")
                bot = pgsql_db_cursor.fetchall()
                # print('******',bot,'******')
                # pgsql_db_cursor.execute(insert_query, values)
                pgsql_db_conn.commit()
            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
    pgsql_db_cursor.close()
    pgsql_db_conn.close()
    # exit()


def recon():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'recon' in indices:
        drop = f"DROP TABLE IF EXISTS recon;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='recon', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS recon (
                        ID VARCHAR(500) PRIMARY KEY,
                        family VARCHAR(500),
                        species VARCHAR(500),
                        product VARCHAR(500),
                        taskName VARCHAR(500),
                        displayName VARCHAR(500),
                        updated TIMESTAMP,
                        created TIMESTAMP
                        );'''
        print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            # print(source_data)
            val = [[val for val in project.values()]
                   for project in source_data]
            value_ty = [str(v) if v != '' else 'null' for v in val]
            column = [[col for col in project.keys()]
                      for project in source_data]
            column = column[0]
            columns = [item.strip().replace(" ", "") for item in column]
            # print(columns)
            for record in source_data:
                ID = str(Id)
                # print(ID)
                # exit()
                family = record['family']
                species = record['species']
                product = record['product']
                taskName = record['taskName']
                displayName = record['displayName']
                updated = record['updated']
                # print(updated)
                created = record['created']
            try:
                insert_query = """INSERT INTO recon (ID, family, species, product, taskName, displayName, updated ,created)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (str(ID) if ID else None,
                          family if family else None,
                          species if species else None,
                          product if product else None,
                          taskName if taskName else None,
                          displayName if displayName else None,
                          updated if updated else None,
                          created if created else None)
                print(values)
                pgsql_db_cursor.execute(insert_query, values)
                print(insert_query)
                print("inserted succesfully")
            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            try:
                pgsql_db_cursor.execute("SELECT * FROM recon")
                # bot = pgsql_db_cursor.fetchall()
                # print('******',bot,'******')
            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()
    # exit()


def recons():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'recons' in indices:
        drop = f"DROP TABLE IF EXISTS recons;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='recons', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS recons (
                        ID VARCHAR(10485760) PRIMARY KEY,
                        reconProcess VARCHAR(10485760),
                        reconName VARCHAR(10485760),
                        sources VARCHAR(10485760),
                        reconId VARCHAR(10485760),
                        displayColumns VARCHAR(10485760),
                        updated TIMESTAMP,
                        created TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            # print(source_data)
            # print(source_data['reconProcess'])
            # source_data = {'reconProcess': reconProcess} if 'reconProcess' in source_data else source_data
            # source_data = {'reconName': reconName} if 'reconName' in source_data else source_data
            # source_data = {'sources': sources} if 'sources' in source_data else source_data
            # source_data = {'reconId': reconId} if 'reconId' in source_data else source_data
            # source_data = {'displayColumns': displayColumns} if 'displayColumns' in source_data else source_data
            # source_data = {'updated': updated} if 'updated' in source_data else source_data
            # source_data = {'created': reconProcess} if 'created' in source_data else source_data
            # source_data.setdefault('reconProcess', reconProcess)
            # source_data.setdefault('reconName', reconName)
            # source_data.setdefault('sources', sources)
            # source_data.setdefault('reconId', reconId)
            # source_data.setdefault('displayColumns', displayColumns)
            # source_data.setdefault('updated', updated)
            # source_data.setdefault('created', created)
            # date_time = datetime.datetime.fromtimestamp( epoch_time )
            # print(source_data)
            # exit()
            for record in source_data:
                ID = str(Id)
                # print(ID)
                # exit()
                reconProcess = record.get('reconProcess', None)
                reconName = record.get('reconName', None)
                sources = record.get('sources', None)
                # print(sources)
                # exit()
                reconId = record.get('reconId', None)
                displayColumns = record.get('displayColumns', None)
                updated = datetime.datetime.fromtimestamp(record['updated'])
                # print(updated)
                created = datetime.datetime.fromtimestamp(record['created'])
            try:
                # values = (
                # ID = "" if not str(ID) else str(ID)
                #   str(ID) if ID else None,
                # reconProcess = "" if not reconProcess else reconProcess
                #   reconProcess if reconProcess else None,
                # reconName  = "" if not reconName else reconName
                #   reconName if reconName else None,
                # sources = "" if not sources else sources
                #   sources if sources else None,
                # reconId = "" if not reconId else reconId
                #   reconId if reconId else None,
                # displayColumns  = "" if not displayColumns else displayColumns
                #   displayColumns if displayColumns else None,
                # updated  = "" if not updated else updated
                #   updated if updated else None,
                # created = "" if not created else created
                #   created if created else None)
                insert_query = """INSERT INTO recons (ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          reconProcess if reconProcess else None,
                          reconName if reconName else None,
                          sources if sources else None,
                          reconId if reconId else None,
                          displayColumns if displayColumns else None,
                          updated if updated else None,
                          created if created else None)
                # print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            try:
                pgsql_db_cursor.execute("SELECT * FROM recons")
                bot = pgsql_db_cursor.fetchall()
                # print('******',bot,'******')
            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def testsmartrecon():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'testsmartrecon' in indices:
        drop = f"DROP TABLE IF EXISTS testsmartrecon;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='testsmartrecon', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS testsmartrecon (
                        ID VARCHAR(1000) PRIMARY KEY,
                        reconId VARCHAR(10485760),
                        source VARCHAR(10485760),
                        count DOUBLE PRECISION,
                        ageing DOUBLE PRECISION,
                        matchedcredit DOUBLE PRECISION,
                        matcheddebit DOUBLE PRECISION,
                        unmatchedcredit DOUBLE PRECISION,
                        unmatcheddebit DOUBLE PRECISION,
                        type VARCHAR(10485760),
                        msg VARCHAR(10485760),
                        reason VARCHAR(10485760),
                        actionStatus BOOLEAN,
                        userAgent VARCHAR(10485760),
                        os VARCHAR(10485760),
                        browser VARCHAR(10485760),
                        device VARCHAR(10485760),
                        os_version VARCHAR(10485760),
                        browser_version VARCHAR(10485760),
                        isMobile VARCHAR(10485760),
                        isTablet VARCHAR(10485760),
                        isDesktopDevice VARCHAR(10485760),
                        email VARCHAR(10485760),
                        businessName VARCHAR(10485760),
                        businessWebsite VARCHAR(10485760),
                        city VARCHAR(10485760),
                        continent VARCHAR(10485760),
                        country VARCHAR(10485760),
                        countryCode VARCHAR(10485760),
                        ipName VARCHAR(10485760),
                        ipType VARCHAR(10485760),
                        isp VARCHAR(10485760),
                        lat VARCHAR(10485760),
                        lon VARCHAR(10485760),
                        org VARCHAR(10485760),
                        query VARCHAR(10485760),
                        region VARCHAR(10485760),
                        status VARCHAR(10485760),
                        family VARCHAR(10485760),
                        species VARCHAR(10485760),
                        updated TIMESTAMP,
                        created TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            # print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            # print(source_data)
            val = [[val for val in project.values()]
                   for project in source_data]
            value_ty = [str(v) if v != '' else 'null' for v in val]
            column = [[col for col in project.keys()]
                      for project in source_data]
            column = column[0]
            columns = [item.strip().replace(" ", "") for item in column]
            # print(columns)
            for record in source_data:
                ID = str(Id)
                # print(ID)
                # exit()
                reconId = record.get('reconId', None)
                source = record.get('source', None)
                count = record.get('count', None)
                ageing = record.get('ageing', None)
                matchedcredit = record.get('matchedcredit', None)
                matcheddebit = record.get('matcheddebit', None)
                unmatchedcredit = record.get('unmatchedcredit', None)
                unmatcheddebit = record.get('unmatcheddebit', None)
                type = record.get('type', None)
                msg = record.get('msg', None)
                reason = record.get('reason', None)
                actionStatus = record.get('actionStatus', None)
                userAgent = record.get('userAgent', None)
                os = record.get('os', None)
                browser = record.get('browser', None)
                device = record.get('device', None)
                os_version = record.get('os_version', None)
                browser_version = record.get('browser_version', None)
                isMobile = record.get('isMobile', None)
                isTablet = record.get('isTablet', None)
                isDesktopDevice = record.get('isDesktopDevice', None)
                email = record.get('email', None)
                businessName = record.get('businessName', None)
                businessWebsite = record.get('businessWebsite', None)
                city = record.get('city', None)
                continent = record.get('continent', None)
                country = record.get('country', None)
                countryCode = record.get('countryCode', None)
                ipName = record.get('ipName', None)
                ipType = record.get('ipType', None)
                isp = record.get('isp', None)
                lat = record.get('lat', None)
                lon = record.get('lon', None)
                org = record.get('org', None)
                query = record.get('query', None)
                region = record.get('region', None)
                status = record.get('status', None)
                family = record.get('family', None)
                species = record.get('species', None)
                updated_int = datetime.datetime.strptime(
                    (record.get('updated')), '%Y-%m-%dT%H:%M:%S')
                timestamp1 = int(updated_int.timestamp())
                updated = datetime.datetime.fromtimestamp(timestamp1, None)
                created_int = datetime.datetime.strptime(
                    (record.get('updated')), '%Y-%m-%dT%H:%M:%S')
                timestamp2 = int(created_int.timestamp())
                created = datetime.datetime.fromtimestamp(timestamp2, None)
                # created = datetime.datetime.fromtimestamp(int('created'), None)
                # print(updated)
            try:
                insert_query = """INSERT INTO testsmartrecon (ID, reconId, source, count, ageing, matchedcredit, matcheddebit,
                unmatchedcredit, unmatcheddebit, type, msg, reason, actionStatus, userAgent, os, browser, device, os_version, 
                browser_version, isMobile, isTablet, isDesktopDevice, email, businessName, businessWebsite, city, continent, country, 
                countryCode, ipName, ipType, isp, lat, lon, org, query, region, status, family, species, updated, created)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                values = (str(ID) if ID else None,
                          reconId if reconId else None,
                          source if source else None,
                          float(count) if count else None,
                          float(ageing) if ageing else None,
                          float(matchedcredit) if matchedcredit else None,
                          float(matcheddebit) if matcheddebit else None,
                          float(unmatchedcredit) if unmatchedcredit else None,
                          float(unmatcheddebit) if unmatcheddebit else None,
                          type if type else None,
                          msg if msg else None,
                          reason if reason else None,
                          bool(actionStatus) if actionStatus else None,
                          userAgent if userAgent else None,
                          os if os else None,
                          browser if browser else None,
                          device if device else None,
                          os_version if os_version else None,
                          browser_version if browser_version else None,
                          isMobile if isMobile else None,
                          isTablet if isTablet else None,
                          isDesktopDevice if isDesktopDevice else None,
                          email if email else None,
                          businessName if businessName else None,
                          businessWebsite if businessWebsite else None,
                          city if city else None,
                          continent if continent else None,
                          country if country else None,
                          countryCode if countryCode else None,
                          ipName if ipName else None,
                          ipType if ipType else None,
                          isp if isp else None,
                          lat if lat else None,
                          lon if lon else None,
                          org if org else None,
                          query if query else None,
                          region if region else None,
                          status if status else None,
                          family if family else None,
                          species if species else None,
                          updated if updated else None,
                          created if created else None,)
                # print(values)
                pgsql_db_cursor.execute(insert_query, values)
                # print(insert_query)
                print("inserted succesfully")
            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            try:
                pgsql_db_cursor.execute("SELECT * FROM testsmartrecon")
                # bot = pgsql_db_cursor.fetchall()
                # print('******',bot,'******')
            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()
    # exit()

    # elk_client_ES = get_elk_conn()
    # indices = list(elk_client_ES.indices.get_alias().keys())
    # sql = '''SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname !='information_schema';'''
    # sql = '''SELECT * FROM gl_balance'''

    # '''SELECT row_to_json(row) FROM (SELECT ID ("Account Number", reconId, reconName, reconProcess, sourceName, stmtDate,
    # "CLOSING BAL", ExecutionId, "OPENING BAL", "Account Name") FROM gl_balance) row;'''
    # '''select reconId::text from gl_balance'''
    # sql3 = '''select * from gl_balance;'''


def flows():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'flows' in indices:
        drop = f"DROP TABLE IF EXISTS flows;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='flows', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS flows (
                        ID VARCHAR(10485760) PRIMARY KEY,
                        reconId VARCHAR(10485760),
                        connections VARCHAR(10485760),
                        nodes VARCHAR(10485760)
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id)
                # print(ID)
                # exit()
                reconId = record.get('reconId', None)
                connections = json.dumps(record["connections"])
                nodes = json.dumps(record["nodes"])
            try:
                insert_query = """INSERT INTO flows (ID, reconId, connections, nodes)
        VALUES (%s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          reconId if reconId else None,
                          connections if connections else None,
                          nodes if nodes else None)
                # print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def source_refrence():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'source_refrence' in indices:
        drop = f"DROP TABLE IF EXISTS source_refrence;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='source_refrence', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS source_refrence (
                        ID VARCHAR(10485760) PRIMARY KEY,
                        delimiter VARCHAR(10485760),
                        structureid VARCHAR(10485760),
                        loadType VARCHAR(10485760),
                        skiprows SMALLINT,
                        skipfooter SMALLINT,
                        structureFileName VARCHAR(10485760),
                        filePattern VARCHAR(10485760),
                        fileExtension VARCHAR(10485760),
                        filters VARCHAR(10485760),
                        derivedColumns VARCHAR(10485760),
                        source VARCHAR(10485760),
                        struct VARCHAR(10485760),
                        fileName VARCHAR(10485760),
                        position VARCHAR(10485760),
                        sourceId VARCHAR(10485760),
                        reconId VARCHAR(10485760),
                        feedDetails VARCHAR(10485760),
                        secondStructure VARCHAR(10485760),
                        masterMergeData VARCHAR(10485760),
                        folderName VARCHAR(10485760),
                        matchCriteria VARCHAR(10485760),
                        enrichColumnsData VARCHAR(10485760)
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id)
                # print(ID)
                # exit()
                delimiter = record.get('reconId', None)
                structureid = record.get('reconId', None)
                loadType = record.get('reconId', None)
                skiprows = record.get(str('skiprows'), None)
                skipfooter = record.get(str('skipfooter'), None)
                structureFileName = record.get('reconId', None)
                filePattern = record.get('reconId', None)
                fileExtension = record.get('reconId', None)
                filters = json.dumps(record['filters'])
                derived_columns = json.dumps(record['derivedColumns'])
                if derived_columns is None:
                    derived_columns = []

                derivedColumns = json.dumps(derived_columns, cls=None)
                source = record.get('reconId', None)
                struct = record.get('reconId', None)
                fileName = record.get('reconId', None)
                position = record.get('reconId', None)
                sourceId = record.get('reconId', None)
                reconId = record.get('reconId', None)
                feedDetails = json.dumps(record['feedDetails'])
                secondStructure = record.get('reconId', None)
                # masterMergeData = json.dumps(record['masterMergeData'])
                master_merge_data = record.get('masterMergeData')
                if master_merge_data is None:
                    master_merge_data = []

                masterMergeData = json.dumps(master_merge_data, cls=None)
                folderName = record.get('folderName', None)
                match_criteria = record.get('matchCriteria')
                if match_criteria is None:
                    match_criteria = []

                matchCriteria = json.dumps(match_criteria, cls=None)
                enrich_columns_data = record.get('enrichColumnsData')
                if enrich_columns_data is None:
                    enrich_columns_data = []

                enrichColumnsData = json.dumps(enrich_columns_data, cls=None)
                # connections =  json.dumps(record["connections"])
                # nodes = json.dumps(record["nodes"])
            try:
                insert_query = """INSERT INTO source_refrence (ID, delimiter,structureid, loadType, skiprows, skipfooter, 
                structureFileName, filePattern, fileExtension, filters, derivedColumns, source, struct, fileName, position, 
                sourceId, reconId, feedDetails, secondStructure, masterMergeData, folderName, matchCriteria, enrichColumnsData)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          delimiter if delimiter else None,
                          structureid if structureid else None,
                          loadType if loadType else None,
                          skiprows if skiprows else None,
                          skipfooter if skipfooter else None,
                          structureFileName if structureFileName else None,
                          filePattern if filePattern else None,
                          fileExtension if fileExtension else None,
                          filters if filters else None,
                          derivedColumns if derivedColumns else None,
                          source if source else None,
                          struct if struct else None,
                          fileName if fileName else None,
                          position if position else None,
                          sourceId if sourceId else None,
                          reconId if reconId else None,
                          feedDetails if feedDetails else None,
                          secondStructure if secondStructure else None,
                          # updated if updated else None,
                          masterMergeData if masterMergeData else None,
                          folderName if folderName else None,
                          matchCriteria if matchCriteria else None,
                          enrichColumnsData if enrichColumnsData else None)
                # print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def recon_execution_details_log():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'recon_execution_details_log' in indices:
        drop = f"DROP TABLE IF EXISTS recon_execution_details_log;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='recon_execution_details_log', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS recon_execution_details_log (
                        ID VARCHAR(1000) PRIMARY KEY,
                        statementDate DATE,
                        jobStatus VARCHAR(1000),
                        errorMsg VARCHAR(1000),
                        reconName VARCHAR(1000),
                        reconProcess VARCHAR(1000),
                        reconId VARCHAR(1000),
                        reconExecutionId VARCHAR(1000),
                        stmtDate TIMESTAMP,
                        executionDateTime TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id)
                stdate = datetime.datetime.strptime(
                    record['statementDate'], "%Y-%m-%d %H:%M:%S")
                statementDate = stdate.strftime("%d%m%Y")
                jobStatus = record.get('jobStatus', None)
                errorMsg = record.get('errorMsg', None)
                reconName = record.get('reconName', None)
                reconProcess = record.get('reconProcess', None)
                reconId = record.get('reconId', None)
                reconExecutionId = record.get('reconExecutionId', None)
                stmtDate = datetime.datetime.fromtimestamp(record['stmtDate'])
                executionDateTime = datetime.datetime.fromtimestamp(
                    int(record['executionDateTime']))
            try:
                insert_query = """INSERT INTO recon_execution_details_log (ID, statementDate, jobStatus, errorMsg, reconName, 
                reconProcess, reconId, reconExecutionId, stmtDate, executionDateTime)
        VALUES (%s, to_date(%s, 'DDMMYYYY'), %s, %s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          statementDate if statementDate else None,
                          jobStatus if jobStatus else None,
                          errorMsg if errorMsg else None,
                          reconName if reconName else None,
                          reconProcess if reconProcess else None,
                          reconId if reconId else None,
                          reconExecutionId if reconExecutionId else None,
                          stmtDate if stmtDate else None,
                          executionDateTime if executionDateTime else None)
                # print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def users():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'users' in indices:
        drop = f"DROP TABLE IF EXISTS users;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='users', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS users (
                        ID VARCHAR(1000) PRIMARY KEY,
                        ApprovalStatus VARCHAR(1000),
                        accountLocked BOOLEAN,
                        loginCount SMALLINT,
                        userName VARCHAR(1000),
                        firstName VARCHAR(1000),
                        lastName VARCHAR(1000),
                        email VARCHAR(1000),
                        password VARCHAR(1000),
                        role VARCHAR(1000),
                        updated TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id)
                ApprovalStatus = record.get('ApprovalStatus', None)
                accountLocked = record.get('accountLocked', None)
                loginCount = record.get('loginCount', None)
                userName = record.get('userName', None)
                firstName = record.get('firstName', None)
                lastName = record.get('lastName', None)
                email = record.get('email', None)
                password = record.get('password', None)
                role = record.get('role', None)
                try:
                    updated = datetime.datetime.fromtimestamp(
                        record['updated'])
                except KeyError:
                    # Handle the case where the 'updated' key is not present in the record dictionary.
                    pass
            try:
                insert_query = """INSERT INTO users (ID, ApprovalStatus, accountLocked, loginCount, userName, firstName, lastName, 
                email, password, role, updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          ApprovalStatus if ApprovalStatus else None,
                          bool(accountLocked) if accountLocked else None,
                          loginCount if loginCount else None,
                          userName if userName else None,
                          firstName if firstName else None,
                          lastName if lastName else None,
                          email if email else None,
                          password if password else None,
                          role if role else None,
                          updated if updated else None)
                print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def source_data():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'source_data' in indices:
        drop = f"DROP TABLE IF EXISTS source_data;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='source_data', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS source_data (
                        ID VARCHAR(1000) PRIMARY KEY,
                        name VARCHAR(1000),
                        type VARCHAR(1000),
                        fileName VARCHAR(1000),
                        created TIMESTAMP,
                        updated TIMESTAMP,
                        filetype VARCHAR(1000),
                        footerkey VARCHAR(1000),
                        headerkey VARCHAR(1000),
                        _id_ VARCHAR(1000),
                        loadType VARCHAR(1000),
                        merge BOOLEAN,
                        parseIndex VARCHAR(1000),
                        skipFooter SMALLINT,
                        skipRow SMALLINT,
                        typeoftransaction VARCHAR(1000),
                        filepassword VARCHAR(1000),
                        password VARCHAR(1000),
                        path VARCHAR(1000),
                        searchkey VARCHAR(1000),
                        searchvalue VARCHAR(1000),
                        username VARCHAR(1000),
                        ip VARCHAR(1000),
                        port SMALLINT,
                        delimeter VARCHAR(1000)
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id)
                name = record.get('name', None)
                type = record.get('type', None)
                fileName = record.get('fileName', None)
                created = datetime.datetime.fromtimestamp(record['created'])
                updated = datetime.datetime.fromtimestamp(record['updated'])
                # updated = record.get('updated',None)
                try:
                    filetype = record.get('filetype', None)
                    footerkey = record.get('footerkey', None)
                    headerkey = record.get('headerkey', None)
                    _id_ = record.get('id', None)
                    loadType = record.get('loadType', None)
                    merge = record.get('merge', None)
                    parseIndex = record.get('parseIndex', None)
                    skipFooter = record.get('skipFooter', None)
                    skipRow = record.get('skipRow', None)
                    typeoftransaction = record.get('typeoftransaction', None)
                    filepassword = record.get('filepassword', None)
                    password = record.get('password', None)
                    path = record.get('path', None)
                    searchkey = record.get('searchkey', None)
                    searchvalue = record.get('searchvalue', None)
                    username = record.get('username', None)
                    ip = record.get('ip', None)
                    port = record.get('port', None)
                    delimeter = record.get('delimeter', None)
                except KeyError:
                    # Handle the case where the 'updated' key is not present in the record dictionary.
                    pass
            try:
                insert_query = """INSERT INTO source_data (ID, name, type, fileName, created, updated, filetype, footerkey, 
                headerkey, _id_, loadType, merge, parseIndex, skipFooter, skipRow, typeoftransaction, filepassword, password, path, 
                searchkey, searchvalue, username, ip, port, delimeter)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          name if name else None,
                          type if type else None,
                          fileName if fileName else None,
                          created if created else None,
                          updated if updated else None,
                          filetype if filetype else None,
                          footerkey if footerkey else None,
                          headerkey if headerkey else None,
                          _id_ if _id_ else None,
                          loadType if loadType else None,
                          bool(merge) if merge else None,
                          parseIndex if parseIndex else None,
                          int(skipFooter) if skipFooter else None,
                          int(skipRow) if skipRow else None,
                          typeoftransaction if typeoftransaction else None,
                          filepassword if filepassword else None,
                          password if password else None,
                          path if path else None,
                          searchkey if searchkey else None,
                          searchvalue if searchvalue else None,
                          username if username else None,
                          ip if ip else None,
                          int(port) if port else None,
                          delimeter if delimeter else None)
                print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def reconaudit():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'reconaudit' in indices:
        drop = f"DROP TABLE IF EXISTS reconaudit;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='reconaudit', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS reconaudit (
                        ID VARCHAR(1000) PRIMARY KEY,
                        type VARCHAR(1000),
                        actionOn VARCHAR(1000),
                        logTime TIMESTAMP,
                        createdBy VARCHAR(1000),
                        created TIMESTAMP,
                        updated TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id)
                type = record.get('type', None),
                actionOn = record.get('actionOn', None),
                time = record['logTime']/1000
                logTime = datetime.datetime.fromtimestamp(time),
                # print(logTime)
                createdBy = record.get('createdBy', None),
                created = datetime.datetime.strptime(
                    str(datetime.datetime.fromtimestamp(record['created'])), "%Y-%m-%d %H:%M:%S"),
                updated = datetime.datetime.strptime(
                    str(datetime.datetime.fromtimestamp(record['updated'])), "%Y-%m-%d %H:%M:%S")
                # print(updated)
                # exit()
                # try:
                #     updated = datetime.datetime.fromtimestamp(record['updated'])
                # except KeyError:
                #     # Handle the case where the 'updated' key is not present in the record dictionary.
                #     pass
            try:
                insert_query = """INSERT INTO reconaudit (ID, type, actionOn, logTime, createdBy, created, updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          type if type else None,
                          actionOn if actionOn else None,
                          logTime if logTime else None,
                          createdBy if createdBy else None,
                          created if created else None,
                          updated if updated else None)
                print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def recon_meta_info():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'recon_meta_info' in indices:
        drop = f"DROP TABLE IF EXISTS recon_meta_info;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='recon_meta_info', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS recon_meta_info (
                        ID VARCHAR(1000) PRIMARY KEY,
                        reconName VARCHAR(1000),
                        reconId VARCHAR(1000),
                        MicroAtm_SwicthFile VARCHAR(1000),
                        MicroAtm_NPCI_FILE VARCHAR(1000),
                        NODAL_DUMP VARCHAR(1000),
                        Aggregator_Dump VARCHAR(1000),
                        VPA_DUMP VARCHAR(1000),
                        AUF_DUMP VARCHAR(1000),
                        E_KUBER VARCHAR(1000),
                        SFMS_INCOMING_GROSSLEVEL VARCHAR(1000),
                        SFMS_OUTGOING_GROSSLEVEL VARCHAR(1000),
                        IPH_OUTWARD VARCHAR(1000),
                        OUTWARD_GL VARCHAR(1000),
                        SFMS_OUTWARD VARCHAR(1000),
                        IPH_INWARD_RETURN VARCHAR(1000),
                        SFMS_INWARD_RETURN VARCHAR(1000),
                        SFMS_INWARD VARCHAR(1000),
                        IPH_INWARD VARCHAR(1000),
                        INWARD_GL VARCHAR(1000),
                        INWARD_RETURN_GL VARCHAR(1000),
                        SFMS_OUTWARD_RETURN VARCHAR(1000),
                        OUTWARD_RETURN_GL VARCHAR(1000),
                        IPH_OUTWARD_RETURN VARCHAR(1000),
                        partnerId VARCHAR(1000),
                        updated TIMESTAMP,
                        created TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id),
                reconName = record.get('reconName', None),
                reconId = record.get('reconId', None),
                MicroAtm_SwicthFile = json.dumps(record.get('MicroAtm_SwicthFile',[])),
                MicroAtm_NPCI_FILE = json.dumps(record.get('MicroAtm_NPCI_FILE',[])),
                NODAL_DUMP = json.dumps(record.get('NODAL DUMP',[])),
                # print(NODAL_DUMP)
                # exit()
                Aggregator_Dump = json.dumps(record.get('Aggregator Dump',[])),
                VPA_DUMP = json.dumps(record.get('VPA_DUMP',[])),
                AUF_DUMP = json.dumps(record.get('AUF_DUMP',[])),
                E_KUBER = json.dumps(record.get('E_KUBER',[])),
                SFMS_INCOMING_GROSSLEVEL = json.dumps(record.get('SFMS_INCOMING_GROSSLEVEL',[])),
                SFMS_OUTGOING_GROSSLEVEL = json.dumps(record.get('SFMS_OUTGOING_GROSSLEVEL',[])),
                IPH_OUTWARD = json.dumps(record.get('IPH_OUTWARD',[])),
                OUTWARD_GL = json.dumps(record.get('OUTWARD_GL',[])),
                SFMS_OUTWARD = json.dumps(record.get('SFMS_OUTWARD',[])),
                IPH_INWARD_RETURN = json.dumps(record.get('IPH_INWARD_RETURN',[])),
                SFMS_INWARD_RETURN = json.dumps(record.get('SFMS_INWARD_RETURN',[])),
                SFMS_INWARD = json.dumps(record.get('SFMS_INWARD',[])),
                IPH_INWARD = json.dumps(record.get('IPH_INWARD',[])),
                INWARD_GL = json.dumps(record.get('INWARD GL',[])),
                INWARD_RETURN_GL = json.dumps(record.get('INWARD_RETURN_GL',[])),
                SFMS_OUTWARD_RETURN = json.dumps(record.get('SFMS_OUTWARD_RETURN',[])),
                OUTWARD_RETURN_GL = json.dumps(record.get('OUTWARD_RETURN_GL',[])),
                IPH_OUTWARD_RETURN = json.dumps(record.get('IPH_OUTWARD_RETURN',[])),
                partnerId = record.get('partnerId', None),
                updated = datetime.datetime.strptime(str(datetime.datetime.fromtimestamp(record['updated'])), "%Y-%m-%d %H:%M:%S"),
                created = datetime.datetime.strptime(str(datetime.datetime.fromtimestamp(record['created'])), "%Y-%m-%d %H:%M:%S")
            try:
                insert_query = """INSERT INTO recon_meta_info (ID, reconName, reconId, MicroAtm_SwicthFile, MicroAtm_NPCI_FILE, 
                NODAL_DUMP,Aggregator_Dump, VPA_DUMP, AUF_DUMP, E_KUBER, SFMS_INCOMING_GROSSLEVEL, SFMS_OUTGOING_GROSSLEVEL, 
                IPH_OUTWARD, OUTWARD_GL, SFMS_OUTWARD, IPH_INWARD_RETURN, SFMS_INWARD_RETURN, SFMS_INWARD, IPH_INWARD, INWARD_GL, 
                INWARD_RETURN_GL, SFMS_OUTWARD_RETURN, OUTWARD_RETURN_GL, IPH_OUTWARD_RETURN, partnerId, updated, created)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (ID if ID else None,
                          reconName if reconName else None,
                          reconId if reconId else None,
                          MicroAtm_SwicthFile if MicroAtm_SwicthFile else None,
                          MicroAtm_NPCI_FILE if MicroAtm_NPCI_FILE else None,
                          NODAL_DUMP if NODAL_DUMP else None,
                          Aggregator_Dump if Aggregator_Dump else None,
                          VPA_DUMP if VPA_DUMP else None,
                          AUF_DUMP if AUF_DUMP else None,
                          E_KUBER if E_KUBER else None,
                          SFMS_INCOMING_GROSSLEVEL if SFMS_INCOMING_GROSSLEVEL else None,
                          SFMS_OUTGOING_GROSSLEVEL if SFMS_OUTGOING_GROSSLEVEL else None,
                          IPH_OUTWARD if IPH_OUTWARD else None,
                          OUTWARD_GL if OUTWARD_GL else None,
                          SFMS_OUTWARD if SFMS_OUTWARD else None,
                          IPH_INWARD_RETURN if IPH_INWARD_RETURN else None,
                          SFMS_INWARD_RETURN if SFMS_INWARD_RETURN else None,
                          SFMS_INWARD if SFMS_INWARD else None,
                          IPH_INWARD if IPH_INWARD else None,
                          INWARD_GL if INWARD_GL else None,
                          INWARD_RETURN_GL if INWARD_RETURN_GL else None,
                          SFMS_OUTWARD_RETURN if SFMS_OUTWARD_RETURN else None,
                          OUTWARD_RETURN_GL if OUTWARD_RETURN_GL else None,
                          IPH_OUTWARD_RETURN if IPH_OUTWARD_RETURN else None,
                          partnerId if partnerId else None,
                          updated if updated else None,
                          created if created else None)
                print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def match_rule_reference():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'match_rule_reference' in indices:
        drop = f"DROP TABLE IF EXISTS match_rule_reference;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='match_rule_reference', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS match_rule_reference (
                        ID VARCHAR(10485760) PRIMARY KEY,
                        reconName VARCHAR(10485760),
                        reconProcess VARCHAR(10485760),
                        reconId VARCHAR(10485760),
                        Rules VARCHAR(10485760),
                        created TIMESTAMP,
                        updated TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id),
                try:
                    reconName = record.get('reconName', None),
                    reconProcess = record.get('reconProcess', None),
                    reconId = record.get('reconId', None),
                    rules = json.dumps(record.get('Rules',[]))
                    created = datetime.datetime.strptime(
                        str(datetime.datetime.fromtimestamp(record['created'])), "%Y-%m-%d %H:%M:%S"),
                    updated = datetime.datetime.strptime(
                        str(datetime.datetime.fromtimestamp(record['updated'])), "%Y-%m-%d %H:%M:%S")
                except KeyError:
                    created = None,
                    updated = None
                    # Handle the case where the 'updated' key is not present in the record dictionary.
                    # passs
                # print(updated)
                # exit()
            try:
                insert_query = """INSERT INTO match_rule_reference (ID, reconName, reconProcess, reconId, Rules, created, updated)
        VALUES (%s, %s, %s, %s, %s,%s, %s);"""  # (ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          reconName if reconName else None,
                          reconProcess if reconProcess else None,
                          reconId if reconId else None,
                          Rules if Rules else None,
                          created if created else None,
                          updated if updated else None)
                # print(values)
                # print(insert_query)
                # exit()
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def recons_metainfo():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'recons_metainfo' in indices:
        drop = f"DROP TABLE IF EXISTS recons_metainfo;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='recons_metainfo', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS recons_metainfo (
                        ID VARCHAR(10485760) PRIMARY KEY,
                        reconName VARCHAR(10485760),
                        reconProcess VARCHAR(10485760),
                        reconId VARCHAR(10485760),
                        reportDetails VARCHAR(10485760),
                        stmtdate TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id)
                reconName = record.get('reconName ', None)
                reconProcess = record.get('reconProcess ', None)
                reconId = record.get('reconId ', None)
                reportDetails = json.dumps(record["reportDetails"])
                stmtdate = datetime.datetime.strptime(
                    str(datetime.datetime.fromtimestamp(record['stmtdate'])), "%Y-%m-%d %H:%M:%S"),
                # print(updated)
                # exit()
                # try:
                #     updated = datetime.datetime.fromtimestamp(record['updated'])
                # except KeyError:
                #     # Handle the case where the 'updated' key is not present in the record dictionary.
                #     pass
            try:
                insert_query = """INSERT INTO recons_metainfo (ID, reconName, reconProcess, reconId, reportDetails, stmtdate)
        VALUES (%s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (str(ID) if ID else None,
                          reconName if reconName else None,
                          reconProcess if reconProcess else None,
                          reconId if reconId else None,
                          reportDetails if reportDetails else None,
                          stmtdate if stmtdate else None)
                print(values)
                # print(insert_query)
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def master_data():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'master_data' in indices:
        drop = f"DROP TABLE IF EXISTS master_data;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='master_data', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS master_data (
                        ID VARCHAR(100) PRIMARY KEY,
                        source VARCHAR(100),
                        created TIMESTAMP,
                        updated TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id),
                source = record.get('source', None),
                print(source)
                # exit()
                created = datetime.datetime.strptime(
                    str(datetime.datetime.fromtimestamp(record['created'])), "%Y-%m-%d %H:%M:%S"),
                updated = datetime.datetime.strptime(
                    str(datetime.datetime.fromtimestamp(record['updated'])), "%Y-%m-%d %H:%M:%S")
            try:
                insert_query = """INSERT INTO master_data (ID, source, created, updated)
        VALUES (%s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (ID if ID else None,
                          source if source else None,
                          created if created else None,
                          updated if updated else None)
                print(values)
                # print(insert_query)
                # exit()
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()


def feed_data():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    elk_client_ES = get_elk_conn()
    indices = list(elk_client_ES.indices.get_alias().keys())
    if 'feed_data' in indices:
        drop = f"DROP TABLE IF EXISTS feed_data;"
        pgsql_db_cursor.execute(drop)
        result = elk_client_ES.search(index='feed_data', size=1000, scroll='1m', body={
                                      "query": {"match_all": {}}})
        data = result['hits']['hits']
        # print(data)
        sample = {}
        create_table_query = '''CREATE TABLE IF NOT EXISTS feed_data (
                        ID VARCHAR(100) PRIMARY KEY,
                        reconName VARCHAR(1000),
                        reconId VARCHAR(1000),
                        reconProcess VARCHAR(1000),
                        emailId VARCHAR(10485760),
                        moveToProcess VARCHAR(10485760),
                        mountPoint VARCHAR(10485760),
                        sftp VARCHAR(10485760),
                        created TIMESTAMP,
                        updated TIMESTAMP
                        );'''
        # print(create_table_query)
        try:
            pgsql_db_cursor.execute(create_table_query)
            # pgsql_db_conn.commit()
            # print("table created")
        except psycopg2.errors.SyntaxError as e:
            print(e)
        for i in range(len(data)):
            sample[i] = data[i]
            Id = sample[i]['_id']
            # print(Id)
            source_data = [sample[i]['_source']]
            for record in source_data:
                ID = str(Id),
                reconName = record.get('reconName', None)
                reconId = record.get('reconId', None)
                reconProcess = record.get('reconProcess', None)
                emailId = record.get('emailId', None)
                moveToProcess = json.dumps(record.get('moveToProcess',[]))
                # print('rer',moveToProcess)
                mountPoint = json.dumps(record.get('mountPoint',[]))
                sftp = json.dumps(record.get('sftp',[]))
                created = datetime.datetime.strptime(str(datetime.datetime.fromtimestamp(record['created'])), "%Y-%m-%d %H:%M:%S"),
                updated = datetime.datetime.strptime(str(datetime.datetime.fromtimestamp(record['updated'])), "%Y-%m-%d %H:%M:%S")
            try:
                insert_query = """INSERT INTO feed_data (ID, reconName, reconId, reconProcess, emailId, moveToProcess, mountPoint, 
                sftp, created, updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""  # ,(ID, reconProcess, reconName, sources, reconId, displayColumns, updated ,created)
                values = (ID if ID else None,
                          reconName if reconName else None,
                          reconId if reconId else None,
                          reconProcess if reconProcess else None,
                          emailId if emailId else None,
                          moveToProcess if moveToProcess else None,
                          mountPoint if mountPoint else None,
                          sftp if sftp else None,
                          created if created else None,
                          updated if updated else None)
                # print(values)
                # print(insert_query)
                # exit()
                pgsql_db_cursor.execute(insert_query, values)
                print("inserted succesfully")

            except psycopg2.errors.SyntaxError as e:
                print(f"Error inserting record: {e}")
            # exit()
            # try:
            #     pgsql_db_cursor.execute("SELECT * FROM flows")
            #     bot = pgsql_db_cursor.fetchall()
            #     # print('******',bot,'******')
            # except psycopg2.errors.SyntaxError as e:
            #     print(f"Error inserting record: {e}")
            # exit()
    pgsql_db_conn.commit()
    pgsql_db_cursor.close()
    pgsql_db_conn.close()

if __name__ == "__main__":
    recon_meta_info()


def pgdata():
    pgsql_db_conn, pgsql_db_cursor = get_db_cursor()
    sql3 = '''SELECT * FROM recons'''
    pgsql_db_cursor.execute(sql3)
    results = pgsql_db_cursor.fetchall()
    for row in results:
        row_dict = dict(zip([desc[0]
                        for desc in pgsql_db_cursor.description], row))
        print(row_dict)
    # for key,value in row_dict.items():
    #     print(value[10])
    pgsql_db_conn.commit()
    pgsql_db_conn.close()
    # bot = pgsql_db_cursor.fetchall()
    # print('******',bot,'******')
# def gl_summary():
#     for index in indices:
#         if index not in ind:
#             # print(index)
#             drop = ('''DROP TABLE IF EXISTS "'''+ index +'''";''')
#             print(drop)
#             pgsql_db_cursor.execute(drop)
#             pgsql_db_conn.commit()
#             result = elk_client_ES.search(index=index, size=1000, scroll='1m', body={"query": {"match_all": {}}})
#             scroll_id = result['_scroll_id']
#             data = result['hits']['hits']
#             data = data[count]
#             count += 1
#             dict = [data['_source']]
#             # print(dict)
#             val = [[value for value in project.values()] for project in dict]
#             column = [[value for value in project.keys()] for project in dict]
#             coll = column[0]
#             columns = [item.strip().replace(" ", "") for item in coll]
#             # print(columns)
#             # print(val)
#             table_create_query = ''
#             for col in columns:
#                 table_create_query += f'"{col}" VARCHAR(500),'
#             table_create_query = table_create_query[:-1]
#             pgsql = '''CREATE TABLE IF NOT EXISTS "''' + index + '''" (''' + table_create_query  + ''')'''
#             print(pgsql)
#             try:
#                 # print(drop)
#                 # pgsql_db_cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname !='information_schema';")
#                 # bot = pgsql_db_cursor.fetchall()
#                 # print(bot)
#                 pgsql_db_cursor.execute(pgsql)
#                 pgsql_db_conn.commit()
#                 print("table created")
#             except psycopg2.errors.SyntaxError as e:
#                 print(e)
#                 exit(0)
#             # print(columns)
#             # print(val)
#             # exit()
#             try:
#                 #pgsql_db_cursor.execute('''SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = "''' + index +'''";''') ("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")

#                 rest = val[0]
#                 value_ty = [str(v) if v != '' else 'null' for v in rest]
#                 value_ty = tuple(value_ty)
#                 print(value_ty)
#                 cc_col = [str(in_column) for in_column in columns]
#                 query = 'INSERT INTO "' + index + '" ("{}") VALUES %s'.format('", "'.join(cc_col))#%s'.format('", "'.join(columns)), '", "'.join(value_ty)
#                 # res = ",".join(columns)
#                 # res = columns[0]
#                 # print(res)
#                 # query = 'INSERT INTO {} ({}) VALUES %s'.format(index, res) #"INSERT INTO {} ({}) values %s on conflict ({}) do nothing".format(index, columns_str, columns_str)# #'INSERT INTO ' + index + ' ({}) VALUES %s'.format('", "'.join(columns_str)) #
#                 # pgsql_db_cursor.execute(query,value_ty)
#                 execute_values(pgsql_db_cursor, query, (value_ty,))
#                 print(query)
#                 # print(query)
#                 pgsql_db_cursor.execute("SELECT * FROM {}".format(index))
#                 bot = pgsql_db_cursor.fetchall()
#                 print('******',bot,'******')
#                 # pgsql_db_cursor = pgsql_db_conn.cursor()

#                 pgsql_db_conn.commit()
#                 pgsql_db_cursor.close()
#                 print("data insert succesfully")
#             except psycopg2.errors.SyntaxError as e:
#                 print(e)


# class DateEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, (date, datetime)):
#             return obj.isoformat()
#         elif isinstance(obj, Decimal):
#             return float(obj)
#         else:
#             return super().default(obj)
