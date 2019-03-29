#coding=utf-8

import datetime

from util.base.read_conf import ReadConf
from util.mysql.mysql import MySql
from Crypto.Cipher import AES
import os,sys
import pymysql
import pyhs2
import ConfigParser
from io import BytesIO
import paramiko
import datetime
import logging
import time

class JobDimDm :

    def __init__(self, job_info, batch_info):
        self.job_info = job_info
        self.batch_info = batch_info
        self.etl_db_conf = ReadConf('dw_service/conf/etl_db_mysql.conf')
        self.mysql_db = MySql(self.etl_db_conf.get_conf())
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    def run(self):
        # 获取作业的相关配置信息
        run_period = self.batch_info['run_period']
        last_runperiod = self.batch_info['last_period']

        job_id = self.job_info['id']
        source_conn_id = self.job_info['source_conn_id']
        check_file_name = self.job_info['source_info1'].replace("${RUNPERIOD}", run_period)
        data_file_name = self.job_info['source_info2'].replace("${RUNPERIOD}", run_period)
        target_conn_id = self.job_info['target_conn_id']
        table_schema = self.job_info['target_info1']
        table_name = self.job_info['target_info2']
        retry_cnt = self.job_info['retry_cnt']
        max_retry_cnt = self.job_info['max_retry_cnt']

        target_conn_sql = "select a.* from hive_etl.etl_conf_conn_account a where a.id = " + str(target_conn_id)
        self.mysql_db.query(target_conn_sql)
        target_conn = self.mysql_db.fetchOneRow()

        hive_host = target_conn['host']
        hive_port = target_conn['port']
        hive_user = target_conn['user_name']
        hive_pw = target_conn['password']
        hive_authmechanism = target_conn['attachment']

        obj = AES.new('This is a key114'.encode('utf-8'), AES.MODE_CBC, 'This is an WZ456'.encode('utf-8'))
        hive_pw = obj.decrypt(hive_pw.decode("string-escape")).rstrip('\0')

