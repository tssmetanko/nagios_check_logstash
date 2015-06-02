#!/usr/bin/env python

import json
import argparse
import socket
import sys
import time
import random
import string
import redis
from datetime import datetime
from elasticsearch import Elasticsearch

# TODO: add ability to set timezone, but now you can export TZ shell variable if you need to change timezone
#export TZ="/usr/share/zoneinfo/America/Chicago"

HEALTH_ID = None
NAGIOS_STATUSES = { 0 : 'OK', 1 : 'WARNING', 2 : 'CRITICAL', 3 : 'UNKNOWN' }

def build_options():
	parser = argparse.ArgumentParser(description='Nagios plugin for check health of logshipment system. \
		Program will send an event to the specific logstash input [redis queue, gelf, file, socket ] \
		and wait until this event appear in Elasticsearch.')
	#---global options
	subparsers = parser.add_subparsers(help='sub-command help')
	parser.add_argument('--critical', '-C', default=3, nargs='?', const=3,
		help='Time lag for detect CRITICAL  (default: 3)')
	parser.add_argument('--warning', '-W', default=2, nargs='?', const=2,
		help='Time lag for detect WARNING (default: 2)')
	parser.add_argument('--es-host', default='localhost', nargs='?', 
		help='Elasticsearch host (default: localhost)')
	parser.add_argument('--es-port', '-P', default=9200, nargs='?', type=int, 
		help='Elasticsearch port (default: 9200)')
	parser.add_argument('--timeout', '-T', default=30, nargs='?', type=int, 
		help='Timeout in seconds for wait answer from ES, or for send heartbeat message. (default: 30)')
	parser.add_argument('--index-time-format', default='%Y.%m.%d', type=str, nargs='?',
		help='ES index time-suffix format. (default: %%Y.%%m.%%d)')
	parser.add_argument('--index-name', default='health-monitor', nargs='?', type=str,
		help='Name of ES index where heartbeat messages should appear. (deafult: health-monitor)')
	#---sub command and options' for <file>
	parser_file = subparsers.add_parser('file', help="file <file_name>")
	parser_file.required = False
	parser_file.add_argument('file', type=str, 
		help="File that monitoried by logstash, where we should send a heartbeat event")
	#---sub command and options for <redis>
	parser_redis = subparsers.add_parser('redis', help="redis -h")
	parser_redis.required = False
	#parser_redis.add_argument('redis', type=str, help='address of redis server')
	parser_redis.add_argument('--redis-host', default='localhost', type=str, 
		help='Redis host where we should send a heartbeat event (default: localhost)')
	parser_redis.add_argument('--redis-port', default=6379, type=int, 
		help='Redis port (default: 6379)')
	parser_redis.add_argument('--redis-db', default='0', type=str, 
		help='Redis database where we should send heartbeat event (default: 0)')
	parser_redis.add_argument('--redis-key', default='logstash-key', nargs='?', type=str,
		help='Redis key where should send the heartbeat event')
	#parset_redis.add_argument('--event-type')
	
	args = parser.parse_args()
	return(args)
	
def get_random_str(length):
	random_data = ''.join(random.SystemRandom().choice(string.ascii_lowercase \
			+ string.digits + string.ascii_uppercase) for _ in range(length))
	return(random_data)

def health_id():
	if not HEALTH_ID:
		global HEALTH_ID 
		HEALTH_ID = get_random_str(32)
	return(HEALTH_ID)
		
def connect_to_redis(host, port=6379, db=0, timeout=30):
	connection = redis.StrictRedis(host=host, port=port, db=db, socket_timeout=timeout)
	if connection.ping():
		return(connection)
	else:
		print('Can\'t establish connection to redis server.')
		return(None)

def connect_to_ES(host,port):
	es = Elasticsearch([
		{'host': host, 'port':port}
	])
	if es.ping():
		return(es)
	else:
		print('Can\'t esatablish connection to ES server.')
		return(None)
	
def build_logstash_message(host=socket.gethostname()):
	current_time_tsmp = time.time()
	message = {
		'version' : "1.1",
		'hostname': host,
		'short_message': "heartbeat of logstash",
		'timestamp': current_time_tsmp,
		'level': 1,
		'type': "health-monitor",
		'message': health_id(),
	}
	return(message)

def send_heartbeat_to_file(file,message):
	send_time = time.time()
	log_file = open(file,'a')
	log_file.write(message + '\n')
	log_file.close()
	return(send_time)

def clean_heartbeat_file(file):
	#for preventation of file grows, we can clean this file. 
	log_file = open(file,'w')
	log_file.write('')
	log_file.close()
	
def send_heardbeat_to_redis(redis_connection,key_name,message):
	#--the `redis_connect` variable should be a result of `connect_to_redis` function 
	#--the `message` variable should be result of `build_logstash_message` function
	send_time = time.time()
	message = json.dumps(message, sort_keys=True, separators=(',', ': '))
	#print(redis_connection)
	if redis_connection.lpush(key_name, message):
		return(send_time)
	else:
		print('Can\'t sent heartbeat event')
		return(None)

def read_heartbeat_from_elasticsearch(es_connection, timeout, time_format, index_name):
	matches = None
	index_date = datetime.now().strftime(time_format)
	index = "%s-%s" % (index_name,index_date)
	query = 'message : "%s"' % health_id()
	for i in range(timeout):
		matches = es_connection.search(index=index, q=query, size=1)
		hits = matches['hits']['hits']
		if not hits:
			time.sleep(1)
		else: 
			break
	else:
		print("timeout ecceded")
		return(None)
	time_to_receive = time.time()
	return(time_to_receive)

def nagios_event(message,status):
	print('%s - %s' % (NAGIOS_STATUSES[status], message))
	sys.exit(status)

cmd_options = build_options()

if 'redis_host' in vars(cmd_options):
	rediska = connect_to_redis(
		host=cmd_options.redis_host, 
		port=cmd_options.redis_port, 
		db=cmd_options.redis_db, 
		timeout=cmd_options.timeout
	)
	heartbeat_message = build_logstash_message()
	time_of_send = send_heardbeat_to_redis(rediska,key_name=cmd_options.redis_key,message=heartbeat_message)
elif 'file' in vars(cmd_options):
	heartbeat_message = health_id()
	time_of_send = send_heartbeat_to_file(cmd_options.file,heartbeat_message)
	clean_heartbeat_file(cmd_options.file)
	
	
es_connection = connect_to_ES(host=cmd_options.es_host,port=cmd_options.es_port)
time_of_receive = read_heartbeat_from_elasticsearch(
	es_connection=es_connection, 
	timeout=cmd_options.timeout, 
	time_format=cmd_options.index_time_format, 
	index_name=cmd_options.index_name
)

time_lag = time_of_receive - time_of_send

#clean temporary data
if 'file' in vars(cmd_options):
	# we can't perform this step in the same 'if' statement above,
	# because logstash does not have time to process event before we clean log.
	# So, let's do it later, after we receive responce from ES.
	clean_heartbeat_file(cmd_options.file)

#send nagios messages and exit
nagios_message = 'the time latency of log shipment is - %f' % (time_lag)
if time_lag >= cmd_options.critical:
	nagios_status = 2
elif time_lag >= cmd_options.warning:
	nagios_status = 1
elif time_lag < cmd_options.warning:
	nagios_status = 0
else:
	nagios_status = 3
	nagios_message = 'Unexpected error'

nagios_event(nagios_message,nagios_status)
	
	
