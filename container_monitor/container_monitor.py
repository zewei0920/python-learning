#!/usr/bin/env python
# -!- coding:UTF-8 -!-

import os
import logging
import shlex
import subprocess
import sys
import time
from multiprocessing.dummy import Pool as ThreadPool
import pickledb
import requests

DB_NAME = 'monitor.db'
SUBPROCESS_TIMEOUT_SECONDS = 30

DBKEY_CONTAINER_STARTING_COUNT = 'CONTAINER_STARTING_COUNT_%s'
DBKEY_CONTAINER_FAILED_COUNT = 'CONTAINER_FAILED_COUNT_%s'

MONITOR_CONTAINER = [{
    'name': 'hadoop',
    'method': 'http',
    'param': 'http://127.0.0.1:50070',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}, {
    'name': 'hbase',
    'method': 'http',
    'param': 'http://127.0.0.1:16010',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}, {
    'name': 'storm',
    'method': 'http',
    'param': 'http://127.0.0.1:8886/index.html',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}, {
    'name': 'pgadmin',
    'method': 'http',
    'param': 'http://127.0.0.1:8001',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}, {
    'name': 'elk',
    'method': 'http',
    'param': 'http://127.0.0.1:5601',
    'starting_wait_max_times': 5,
    'failed_max_times': 3
}, {
    'name': 'nginx',
    'method': 'http',
    'param': 'http://127.0.0.1',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}, {
    'name': 'redis1',
    'method': 'cmd',
    'param': 'docker exec redis1 redis-cli -h redis1 -a dpp123456+_) ping',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}, {
    'name': 'redis2',
    'method': 'cmd',
    'param': 'docker exec redis2 redis-cli -h redis2 -a dpp123456+_) ping',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}, {
    'name': 'postgresql',
    'method': 'cmd',
    'param': 'docker exec postgresql psql -U pgsql -d pgsql -c "select 1" -o /dev/null',
    'starting_wait_max_times': 3,
    'failed_max_times': 3
}]


def log_init():
    logger = logging.getLogger(sys.argv[0])
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    #logfile = str(logger.name).split('.')[0] + '-' + time.strftime('%Y%m%d', time.localtime(time.time())) + '.log'
    logfile = time.strftime('%Y%m%d', time.localtime(time.time())) + '.log'

    # file handler config
    file_handler = logging.FileHandler(sys.path[0] + '/' + logfile, mode='a+')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # console handler config
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    return logger


def db_init():
    try:
        db = pickledb.load(sys.path[0] + '/' + DB_NAME, False)
    except:
        logger.error(sys.exc_info()[0])
    else:
        logger.info("open db file {} successfully !".format(DB_NAME))
    return db


class ContainerMonitor:
    def __init__(self, service):
        self.name = service['name']
        self.method = service['method']
        self.param = service['param']
        self.starting_wait_max_times = service['starting_wait_max_times']
        self.failed_max_times = service['failed_max_times']

    def is_continer_no_exist(self):
        ret = subprocess.check_output(['docker', 'ps', '-a', '-f', 'name=%s' % self.name, '-q'], SUBPROCESS_TIMEOUT_SECONDS)
	if 0 < len(ret):
            return False
        else:
            return True

    def is_container_starting(self):
        if db.exists(DBKEY_CONTAINER_STARTING_COUNT % self.name):
            count = db.get(DBKEY_CONTAINER_STARTING_COUNT % self.name)
            if count > 0:
                db.set(DBKEY_CONTAINER_STARTING_COUNT % self.name, count - 1)
                return True
            else:
                return False
        else:
            return False

    def is_container_no_response_by_http(self):
        try:
            resp_http = requests.head(self.param, timeout=SUBPROCESS_TIMEOUT_SECONDS)
            if resp_http.status_code >= 400:
                return True
        except:
            logger.debug("{} does't reponse by http: {}".format(self.name, sys.exc_info()[0]))
            return True
        else:
            return False

    def is_container_no_response_by_cmd(self):
        return subprocess.call(shlex.split(self.param), SUBPROCESS_TIMEOUT_SECONDS) != 0

    def is_container_no_response(self):
        if self.method == 'http':
            return self.is_container_no_response_by_http()
        else:
            return self.is_container_no_response_by_cmd()


    def restart_container(self):
        db.set(DBKEY_CONTAINER_STARTING_COUNT % self.name, self.starting_wait_max_times)
        db.set(DBKEY_CONTAINER_FAILED_COUNT % self.name, 0)
        subprocess.call(['docker', 'restart', self.name])

    def monitor_exec(self):
        logger.debug("monitor container: {}".format(self.name))
        if self.is_continer_no_exist():
            logger.error("{} does not exist !".format(self.name))
        elif self.is_container_starting():
            logger.warning("{} is starting !".format(self.name))
        elif self.is_container_no_response():
            if db.exists(DBKEY_CONTAINER_FAILED_COUNT % self.name):
		container_failed_count = db.get(DBKEY_CONTAINER_FAILED_COUNT % self.name)
            	if container_failed_count < self.failed_max_times:
                    db.set(DBKEY_CONTAINER_FAILED_COUNT % self.name, container_failed_count + 1)
                    logger.warning("{} does't reponse for {}th times !".format(self.name, container_failed_count + 1))
                else:
	            self.restart_container();
                    logger.warning("{} has been restarted !".format(self.name, time.ctime()))
	    else:
                db.set(DBKEY_CONTAINER_FAILED_COUNT % self.name, 1)
                logger.warning("{} does't reponse for {}th times !".format(self.name, 1))
        else:
            logger.info("{} is running !".format(self.name))

try:
    # start to execute
    logger = log_init()
    logger.info("Now start to execute script: {} !".format(sys.argv[0]))
    db = db_init()

    # ThreadPool
    service_num = len(MONITOR_CONTAINER)
    pool = ThreadPool(service_num)
    for i in range(0, service_num):
        pool.apply_async(ContainerMonitor(MONITOR_CONTAINER[i]).monitor_exec())
    pool.close()
    pool.join()
except:
    logger.error(sys.exc_info()[0])
finally:
    db.dump()
    logger.info("Now exit !")


