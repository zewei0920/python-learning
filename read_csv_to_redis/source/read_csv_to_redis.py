#!/usr/bin/python

import os
import sys
import time
import redis
import configparser
import logging

CFG = \
{
    'interval':'5',
    'skiplinenum':'0',
    'datadir':'./data/',
    'plants':'DPP_02_00000001',
    'host':'127.0.0.1',
    'port':'6379',
    'dbindex':'0',
    'password':'123456',
}

logger = logging.getLogger(sys.argv[0])

def config_log():
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    logfile = logger.name + '-' + time.strftime('%Y%m%d%H%M', time.localtime(time.time())) + '.log'
    # file handler config
    file_handler = logging.FileHandler(logfile, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # console handler config
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)

class ReadFromCsv:
    def __init__(self, dir, name, offset=0):
        self.dir = dir
        self.name = name
        self.offset = offset

    def _process_one_line(self, fp):
        fp.seek(self.offset)
        line = fp.readline()
        self.offset += len(line)
        line = line.replace('\n', '').replace('\t', '').replace('\r', '').replace('null', '0')
        while line == '':
            line = fp.readline()
            self.offset += len(line)
            line = line.replace('\n', '').replace('\t', '').replace('\r', '').replace('null', '0')
        if line == '':
            logger.info('{} has been read to end. offset: {}'.format(self.name, self.offset))
            self.offset = 0
        logger.debug('{} readline: {}'.format(self.name, line))
        return line

    def read_line(self, delimiter, index):
        filepath = os.path.join(os.path.abspath(self.dir), self.name)
        try:
            fp = open(filepath, 'r')
        except IOError as e:
            logger.error("{}".format(e))
            return ""
        return self._process_one_line(fp).split(delimiter)[index]


class WriteToRedis:
    def __init__(self, cfgfile):
        self.cfgfile = cfgfile
        try:
            open(cfgfile, mode='r')
        except IOError:
            logger.error("Failed to open {} !".format(cfgfile))
        self._read_config()

    def _read_config(self):
        cur_path = os.path.abspath(os.path.curdir)
        configparser.RawConfigParser(allow_no_value=True)
        config = configparser.ConfigParser()
        config.read(os.path.join(cur_path, self.cfgfile), encoding="utf-8")
        try:
            CFG['interval'] = config.get('app', 'interval')
            CFG['skiplinenum'] = config.get('app', 'skiplinenum')
            CFG['datadir'] = config.get('app', 'datadir')
            CFG['plants'] = config.get('app', 'plants').__str__().split('#')
            CFG['host'] = config.get('redis', 'host')
            CFG['port'] = config.get('redis', 'port')
            CFG['dbindex'] = config.get('redis', 'dbindex')
            CFG['password'] = config.get('redis', 'password')
        except configparser.Error as e:
            logger.error("{}".format(e))
        finally:
            logger.debug("###### current configuration info:")
            for key, value in CFG.items():
                logger.debug("{}:{}".format(key, value))

    def _read_from_csv(self, reader_set):
        sensor_value_map = {}
        # read only one line from each sensor data file one by one
        for reader in reader_set:
            value = reader.read_line(',', 1)
            sensor_id = reader.name.strip('.')
            sensor_value_map[sensor_id] = value
        logger.debug("sensor_value_map:{}".format(sensor_value_map))
        return sensor_value_map

    def write_to_redis(self):
        # init redis
        redis_client = redis.Redis(host=CFG['host'], port=CFG['port'],db=CFG['dbindex'], password=CFG['password'])
        redis_pipe = redis_client.pipeline()

        # init read set
        reader_set = []
        data_file_list = os.listdir(CFG['datadir'])
        logger.debug("date file list: {}".format(data_file_list))
        for file in data_file_list:
            reader_set.append(ReadFromCsv(CFG['datadir'], file))

        # always run this loop until the script is killed
        while True:
            start_time = time.time()
            # read sensor info from csv file
            sensor_value_map = self._read_from_csv(reader_set)
            
            # write to redis
            for sensor, value in sensor_value_map.items():
                timestamp = int(start_time * 1000)
                redis_value = "{'sensorID':%s,'timestamp':%s,'value':%s,'quality':0}" % (sensor, timestamp, value)
                for plant in CFG['plants']:
                    if len(plant) == 0:
                        continue
                    redis_pipe.set("{}:{}".format(plant, sensor), redis_value)
                    redis_pipe.execute()
            end_time = time.time()
            logger.info("write {} sensors to redis, use {} seconds.".format(len(sensor_value_map), int(end_time-start_time)))
            # sleep a while
            logger.info("sleep {} seconds......".format(CFG['interval']))
            time.sleep(float(CFG['interval']))
        # save redis to disk
        redis_client.save()


if __name__ == '__main__':
    config_log()
    logger.info("Execute script '{}':".format(sys.argv[0]))
    WriteToRedis('app.config').write_to_redis()

