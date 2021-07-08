import csv
import time
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers
import boto3
import yaml
import os
import multiprocessing
import shutil

def run_parallel():
    config     = parse_config('pulse_config.yaml')
    input_list = get_input_list(config)
    num_cores  = multiprocessing.cpu_count()
    if num_cores > len(input_list):
        num_processes = len(input_list)
    else:
        num_processes = num_cores
    print('Running Parallel...')
    pool = multiprocessing.Pool(processes = num_processes)
    pool.map(run_single, input_list)
    return {'statusCode': 200}

def run_single(inputs):
    print('Working on project: {}'.format(inputs['project']))
    pull_date  = (datetime.today() + timedelta(days = -11)).strftime('%Y-%m-%d')
    gte, lte   = get_gte_lte(pull_date)
    query      = build_query(inputs['query'], gte, lte)
    data       = get_data(query)
    clean_data = sort_data(data, pull_date, inputs)
    clean_data = alter_time_format(clean_data)
    clean_data.insert(0, inputs['columns'])
    local_file = write_to_csv(inputs['project'], clean_data, pull_date)
    print('Downloaded {} records for project {} to {}'.format(len(clean_data)-1, inputs['project'], local_file))
    #delete_local_file(local_file)

def parse_config(config_path):
    config_file = open(config_path, 'r')
    return yaml.load(config_file, Loader=yaml.FullLoader)

def get_input_list(config):
    projects = []
    for effort in config.keys():
        campaigns = config[effort].keys()
        for campaign in campaigns:
            save_bucket = config[effort][campaign]['save_bucket']
            columns     = config[effort][campaign]['columns']
            for project in config[effort][campaign]['projects'].keys():
                project_input = {
                    'project'     : project,
                    'save_bucket' : save_bucket,
                    'columns'     : columns,
                    'query'       : config[effort][campaign]['projects'][project]['query'],
                    'key'         : config[effort][campaign]['projects'][project]['key'],
                    'pulse_proj'  : config[effort][campaign]['projects'][project]['pulse_proj'] if 'pulse_proj' in config[effort][campaign]['projects'][project].keys() else None
                }
                projects.append(project_input)
    return projects

def build_query(root_query, gte, lte):
    query_string = {}
    query_raw = {}
    query_raw["query"] = root_query
    query_raw["analyze_wildcard"] = "true"
    query_string["query_string"] = query_raw
    range_string = {}
    range_raw = {}
    timestamp = {}
    timestamp["gte"] = gte
    timestamp["lte"] = lte
    timestamp["format"] = "epoch_millis"
    range_raw["norm.timestamp"] = timestamp
    range_string["range"] = range_raw
    must_fields = {}
    must_fields["must"] = [query_string, range_string]
    must_fields["filter"] = []
    must_fields["should"] = []
    must_fields["must_not"] = []
    boolean_fields = {}
    boolean_fields["bool"] = must_fields
    query = {}
    query["version"] = "true"
    source_fields = {}
    source_fields["includes"] = ["norm", "type", "doc"]
    source_fields["excludes"] = []
    query["_source"] = source_fields
    query["query"] = boolean_fields
    return query

def get_gte_lte(date_str):
    utc = datetime.strptime(date_str, '%Y-%m-%d')
    gte = str(utc.timestamp())[:10] + '000'
    lte = str((utc + timedelta(days = 1) - timedelta(seconds = 1)).timestamp())[:10] + '000'
    return gte, lte

def get_data(query):
    es_string = 'https://rays-query:HcpERBO39KclPbstx9pG@c7c3fe0d2c574fddaef095e5ecb22690.us-east-1.aws.found.io:9243'
    es = Elasticsearch(es_string, timeout=360)
    return [doc for doc in helpers.scan(es, index='pulse-rays', query=query)]

def sort_data(data, date_pulled, input_dict):
    clean = []
    for doc in data:
        try:
            doc_type = doc['_source']["type"]
        except KeyError:
            doc_type = None
            continue
        if ('telegram' in doc_type):
            num_followers = None
            num_posts     = None
            try:
                url = doc['_source']['norm']['url']
            except:
                url = doc['_source']['norm']['domain']
            try:
                content = doc['_source']['norm']['body']
            except:
                content = None
            if 'tweet' in doc_type:
                platform         = 'tw'
                parent, doc_type = parse_tw(doc)
                num_followers    = doc['_source']['doc']['user']['followers_count']
                num_posts        = doc['_source']['doc']['user']['statuses_count']
            elif 'fbgraph' in doc_type:
                platform = 'fb'
                parent   = parse_fb(doc)
            elif 'instagram' in doc_type:
                platform = 'instagram'
                parent   = parse_inst(doc)
            elif 'youtube' in doc_type:
                platform = 'youtube'
                parent   = parse_yt(doc)
            elif 'vk' in doc_type:
                platform = 'vk'
                parent   = parse_vk(doc)
            elif 'telegram' in doc_type:
                platform = 'telegram'
                parent    = doc['_source']['doc']['group']
            else:
                platform = 'unknown'
                parent   = None
            lang = get_lang(doc)
            pulse_proj = input_dict['pulse_proj'] if input_dict['pulse_proj'] is not None else input_dict['project']
            rule_tag, rule_author = get_rule_info(pulse_proj, doc)
            result_dict = {
                'contentid'    : doc['_source']['norm']['id'],
                'content'      : content,
                'authoreddate' : doc["_source"]['norm']['timestamp'],
                'language'     : lang,
                'permalink'    : url,
                'platform'     : platform,
                'author'       : doc['_source']['norm']['author'],
                'doc_type'     : doc_type,
                'parentid'     : parent,
                'num_followers': num_followers,
                'num_posts'    : num_posts,
                'date_pulled'  : date_pulled,
                'project'      : input_dict['project'],
                'rule_tag'     : rule_tag
            }
            row = get_row(result_dict, input_dict['columns'])
            clean.append(row)
    return clean

def get_rule_info(project, doc):
    if 'meta' in doc['_source'].keys():
        for result in doc['_source']['meta']['rule_matcher'][0]['results']:
            try:
                proj = result['metadata']['project_title']
                if project == proj:
                    rule_tag = result['rule_tag']
                    try:
                        rule_author = result['metadata']['username']
                    except:
                        rule_author = None
                    return rule_tag, rule_author
            except:
                return None, None
        return None, None
    else:
        return None, None

def get_row(result_dict, columns):
    return [result_dict[item] if item in result_dict.keys() else None for item in columns]

def parse_tw(doc):
    tweet_doc = doc['_source']['doc']
    if 'quoted_status' in tweet_doc and 'retweeted_status' not in tweet_doc:
        parent     = tweet_doc['quoted_status_id_str']
        tweet_type = 'quoted_tweet'
    elif tweet_doc['in_reply_to_status_id_str'] is not None:
        parent     = tweet_doc['in_reply_to_status_id_str']
        tweet_type = 'tweet_reply'
    else:
        try:
            parent     = tweet_doc['retweeted_status']['id_str']
            tweet_type = 'tweet_retweet'
        except KeyError:
            parent     = None
            tweet_type = 'tweet_original'
    return parent, tweet_type

def parse_fb(doc):
    try:
        parent = doc['_source']['doc']['parent']
    except KeyError:
        parent = None
    return parent

def parse_inst(doc):
    try:
        parent = doc['_source']['doc']['parent_id']
    except KeyError:
        parent = None
    return parent

def parse_yt(doc):
    try:
        parent = doc['_source']['doc']['snippet']['videoId']
    except KeyError:
        parent = None
    return parent

def parse_vk(doc):
    try:
        parent = doc['_source']['doc']['post_id']
    except KeyError:
        parent = None
    return parent

def get_lang(doc):
    try:
        #return doc['_source']['meta']['body_language'][0]['results'][0]['value']
        return doc['_source']['doc']['lang']
    except:
        return None

def alter_time_format(content):
    date_patterns = ['%Y-%m-%dT%H:%M:%S.%f+00:00', '%Y-%m-%dT%H:%M:%S+00:00', '%Y-%m-%dT%H:%M:%S.%f']
    for row in range(len(content)):
        for pattern in date_patterns:
            try:
                content[row][2] = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(content[row][2], pattern))
            except:
                pass
    return content

def write_to_csv(project, clean_data, pull_date):
    if os.path.exists(project):
        shutil.rmtree(project)
    os.mkdir(project)
    local_file = './{}/{}.csv'.format(project, pull_date)
    f = open(local_file, 'w', newline = '', encoding='utf-8')
    writer = csv.writer(f)
    writer.writerows(clean_data)
    f.close()
    return local_file

def delete_local_file(local_file):
    shutil.rmtree(os.path.dirname(local_file))

if __name__ == '__main__':
    run_parallel()
