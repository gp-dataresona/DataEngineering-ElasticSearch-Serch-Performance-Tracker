import boto3
import urllib.request
from itertools import chain
from collections import defaultdict
import json
import itertools
import datetime
import os
from time import gmtime, strftime, sleep
import shutil
import sys

# Author- gautham panchumarthi

# S3 /boto3 related variables
s3_object = boto3.client('s3')
s3 = boto3.resource('s3')
client = boto3.client('s3')

# Environmental Variables
bucketName = os.environ['bucketName']
domain = os.environ['domain']
subDirToReadFile = os.environ['subDirToReadFile']
region = os.environ["region"]
subDirToWriteFile = os.environ["subDirToWriteFile"]

# Global Variable
last_added_file = ""
previous_added_file = ""


# TODO -
# - Change the env variable value for - "subDirToReadFile" back to actual dir

def lambda_handler(event, context):
    test_raw_response = getCurrentRawJsonFile()
    print(test_raw_response.keys())
    index_filter(getCurrentRawJsonFile())


def getLastModifiedFileName():
    # Get the latest updated object in the bucket.
    try:
        get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
        objs = s3_object.list_objects_v2(Bucket=bucketName, Prefix=domain + "/" + subDirToReadFile)['Contents']
        last_added_file_withpath = [obj['Key'] for obj in sorted(objs, key=get_last_modified, reverse=True)][0]
        print("last_added_file_withpath")
        print(last_added_file_withpath)

        global last_added_file
        last_added_file = last_added_file_withpath.split('/')[-1]
        last_added_time = last_added_file.split('_')[1].split('.')[0]

    except Exception as E:
        print("Couldn't find the relevant direcotry or the file to read")
        print("Exception: " + str(E))
        sys.exit(1)
    print(last_added_file)
    return last_added_file_withpath


def getPreviousModifiedFileName():
    # Get the second latest updated object in the bucket.
    try:
        get_previous_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
        objs = s3_object.list_objects_v2(Bucket=bucketName, Prefix=domain + "/" + subDirToReadFile)['Contents']
        previous_added_file_withpath = [obj['Key'] for obj in sorted(objs, key=get_previous_modified, reverse=True)][1]
        print("previous_added_file_withpath")
        print(previous_added_file_withpath)

        global previous_added_file
        previous_added_file = previous_added_file_withpath.split('/')[-1]
        previous_added_time = previous_added_file.split('_')[1].split('.')[0]
    except Exception as E:
        print("Couldn't find the relevant direcotry or the file to read")
        print("Exception: " + str(E))
        sys.exit(1)
    print("previous_added_file")
    print(previous_added_file)
    return previous_added_file_withpath


def getCurrentRawJsonFile():
    try:
        # Reads/saves the latest added json from the S3 bucket
        file_key = getLastModifiedFileName()
        obj = s3_object.get_object(Bucket=bucketName, Key=file_key)
        loaded_data = json.loads(obj['Body'].read().decode('utf-8'))
        # TODO - Remove this keys print
        print("getRawJasonFile Keys")
        print(loaded_data.keys())
    except Exception as E:
        print("Unable to download the file from S3 bucket")
        print("Exception: " + str(E))
        sys.exit(1)
    return loaded_data


def getPreviousRawJsonFile():
    try:
        # Reads/saves the second latest added json from the S3 bucket
        file_key = getPreviousModifiedFileName()
        obj = s3_object.get_object(Bucket=bucketName, Key=file_key)
        loaded_data = json.loads(obj['Body'].read().decode('utf-8'))
        # TODO - Remove this keys print
        print("getPreviousModifiedFileName Keys")
        print(loaded_data.keys())
    except Exception as E:
        print("Unable to download the file from S3 bucket")
        print("Exception: " + str(E))
        sys.exit(1)
    return loaded_data

def index_filter(rawJsonFile):
    # Raw response parsing helper variables
    desired_dict_v1 = {}
    final_dict = {}
    grouped_items = {}
    arr = []
    app = {}
    index_list = rawJsonFile['indices']
    index_counts = len(index_list)

    # Total query perf details
    total_cluster_open_contexts_kv = dict(itertools.islice(rawJsonFile['_all']['total']['search'].items(), 1))
    total_cluster_open_contexts_kv["total_cluster_open_contexts"] = total_cluster_open_contexts_kv.pop("open_contexts")
    print("total_cluster_open_contexts_kv")
    print(total_cluster_open_contexts_kv)

    total_cluster_queries_kv = dict(itertools.islice(rawJsonFile['_all']['total']['search'].items(), 1, 2))
    total_cluster_queries_kv["total_cluster_queries"] = total_cluster_queries_kv.pop("query_total")
    print("total_cluster_queries_kv")
    print(total_cluster_queries_kv)

    total_cluster_query_time_in_millis_kv = dict(itertools.islice(rawJsonFile['_all']['total']['search'].items(), 2, 3))
    total_cluster_query_time_in_millis_kv[
        "total_cluster_query_time_in_millis"] = total_cluster_query_time_in_millis_kv.pop("query_time_in_millis")
    print("total_cluster_query_time_in_millis_kv")
    print(total_cluster_query_time_in_millis_kv)

    total_cluster_query_current_kv = dict(itertools.islice(rawJsonFile['_all']['total']['search'].items(), 3, 4))
    total_cluster_query_current_kv["total_cluster_query_current"] = total_cluster_query_current_kv.pop("query_current")
    print("total_cluster_query_current_kv")
    print(total_cluster_query_current_kv)

    total_cluster_fetch_total_kv = dict(itertools.islice(rawJsonFile['_all']['total']['search'].items(), 4, 5))
    total_cluster_fetch_total_kv["total_cluster_fetch_total"] = total_cluster_fetch_total_kv.pop("fetch_total")
    print("total_cluster_fetch_total_kv")
    print("total_cluster_fetch_total_kv")

    total_cluster_fetch_time_in_millis_kv = dict(itertools.islice(rawJsonFile['_all']['total']['search'].items(), 5, 6))
    total_cluster_fetch_time_in_millis_kv[
        "total_cluster_fetch_time_in_millis"] = total_cluster_fetch_time_in_millis_kv.pop("fetch_time_in_millis")
    print("total_cluster_fetch_time_in_millis_kv")
    print(total_cluster_fetch_time_in_millis_kv)

    total_cluster_fetch_current_kv = dict(itertools.islice(rawJsonFile['_all']['total']['search'].items(), 6, 7))
    total_cluster_fetch_current_kv["total_cluster_fetch_current"] = total_cluster_fetch_current_kv.pop("fetch_current")
    print("total_cluster_fetch_current_kv")
    print(total_cluster_fetch_current_kv)

    total_appname = {'application': 'total_summary_app'}

    # total_store_size_kv["total_size"] = total_store_size_kv["total_size"] / (1073741824) # (1024*1024*1024 = 1073741824)

    total_summary_dict = dict(list(total_appname.items()) + list(total_cluster_open_contexts_kv.items()) + list(
        total_cluster_queries_kv.items()) + list(total_cluster_query_time_in_millis_kv.items())
                              + list(total_cluster_query_current_kv.items()) + list(
        total_cluster_fetch_total_kv.items()) + list(total_cluster_fetch_time_in_millis_kv.items()) + list(
        total_cluster_fetch_current_kv.items()))

    print("Total Summary Stats of ES")
    print(total_summary_dict)

    # Parsing the index values and gives the result having following values
    # try:
    not_accepted_values = ['restored', 'restore', 'shardfix']
    for (outer_k, outer_v) in rawJsonFile["indices"].items():
        # print(outer_k);
        index_split = outer_k.split("-");
        index_sub_string_1 = index_split[-1];
        index_sub_string_2 = index_split[-1]
        if index_sub_string_1 in not_accepted_values:
            appname = outer_k.rsplit('-', 2)
            newapplicationname = appname[0]
        else:
            appname = outer_k.rsplit('-', 1)
            newapplicationname = appname[0]
            # print(newapplicationname)

            index_name_kv = {"application": newapplicationname}

            index_open_contexts_kv = dict(itertools.islice(outer_v['total']['search'].items(), 1))
            index_open_contexts_kv["index_open_contexts"] = index_open_contexts_kv.pop("open_contexts")
            # print("index_open_contexts_kv")
            # print(index_open_contexts_kv)

            index_queries_kv = dict(itertools.islice(outer_v['total']['search'].items(), 1, 2))
            index_queries_kv["index_query_total"] = index_queries_kv.pop("query_total")
            # print("index_queries_kv")
            # print(index_queries_kv)

            index_query_time_in_millis_kv = dict(itertools.islice(outer_v['total']['search'].items(), 2, 3))
            index_query_time_in_millis_kv["index_query_time_in_millis"] = index_query_time_in_millis_kv.pop(
                "query_time_in_millis")
            # print("index_query_time_in_millis_kv")
            # print(index_query_time_in_millis_kv)

            index_query_current_kv = dict(itertools.islice(outer_v['total']['search'].items(), 3, 4))
            index_query_current_kv["index_query_current"] = index_query_current_kv.pop("query_current")
            # print("index_query_current_kv")
            # print(index_query_current_kv)

            index_fetch_total_kv = dict(itertools.islice(outer_v['total']['search'].items(), 4, 5))
            index_fetch_total_kv["index_fetch_total"] = index_fetch_total_kv.pop("fetch_total")
            # print("index_fetch_total_kv")
            # print("index_fetch_total_kv")

            index_fetch_time_in_millis_kv = dict(itertools.islice(outer_v['total']['search'].items(), 5, 6))
            index_fetch_time_in_millis_kv["index_fetch_time_in_millis"] = index_fetch_time_in_millis_kv.pop(
                "fetch_time_in_millis")
            # print("index_fetch_time_in_millis_kv")
            # print(index_fetch_time_in_millis_kv)

            index_fetch_current_kv = dict(itertools.islice(outer_v['total']['search'].items(), 6, 7))
            index_fetch_current_kv["index_fetch_current"] = index_fetch_current_kv.pop("fetch_current")
            # print("index_fetch_current_kv")
            # print(index_fetch_current_kv)

            desired_dict_v1 = dict(list(index_name_kv.items()) + list(index_open_contexts_kv.items()) + list(
                index_queries_kv.items()) + list(index_query_time_in_millis_kv.items())
                                   + list(index_query_current_kv.items()) + list(index_fetch_total_kv.items()) + list(
                index_fetch_time_in_millis_kv.items()) + list(index_fetch_current_kv.items()))

            print(desired_dict_v1)


# Clear the disk by removing the tmp data accumulated by the lambda
removeTmpData()

def removeTmpData():
    for filename in os.listdir("/tmp/"):
        print(filename)
        filepath = os.path.join("/tmp/", filename)
        try:
            shutil.rmtree(filepath)
            print(str(fileName) + " Successfully removed from the '/tmp/' directory")
        except OSError:
            os.remove(filepath)
