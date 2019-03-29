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
import json

u'''
该作业是将ods层数据汇总到dwd层
使用该作业的条件是：ods层的是增量同步的，而且对应的数据表是具有主键的
'''
class JobDwdIncrByKey :

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
        source_table_schema = self.job_info['source_info1'].replace("${RUNPERIOD}", run_period)
        source_table_name = self.job_info['source_info2'].replace("${RUNPERIOD}", run_period)
        target_conn_id = self.job_info['target_conn_id']
        target_table_schema = self.job_info['target_info1']
        target_table_name = self.job_info['target_info2']
        attachment = json.loads(self.job_info['attachment'])
        pkey = attachment['pkey']
        pkey_cond = attachment['pkey_cond']
        retry_cnt = self.job_info['retry_cnt']
        max_retry_cnt = self.job_info['max_retry_cnt']

        source_conn_sql = "select a.* from hive_etl.etl_conf_conn_account a where a.id = " + str(source_conn_id)
        self.mysql_db.query(source_conn_sql)
        source_conn = self.mysql_db.fetchOneRow()

        sftp_host = source_conn['host']
        sftp_port = source_conn['port']
        sftp_name = source_conn['user_name']
        sftp_pw = source_conn['password']
        source_path = source_conn['attachment']

        obj = AES.new('This is a key114'.encode('utf-8'), AES.MODE_CBC, 'This is an WZ456'.encode('utf-8'))
        sftp_pw = obj.decrypt(sftp_pw.decode("string-escape")).rstrip('\0')  # obj.encrypt('123456'.encode('utf-8'))

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

        try :
            hive_conn = pyhs2.connect(host=hive_host, port=hive_port, authMechanism=hive_authmechanism, user=hive_user, password=hive_pw, database=target_table_schema)
            # host主机ip,port：端口号，username:用户名，database:使用的数据库名称
            hive_cursor=hive_conn.cursor()

            # 清空tmp表
            trun_tmp_table = "truncate table " + target_table_schema + "_tmp." + target_table_name
            hive_cursor.execute(trun_tmp_table) # 执行查询

            # dwd表数据移至tmp表
            dwd_to_tmp = "insert into " + target_table_schema + "_tmp." + target_table_name + " select * from " +target_table_schema + "." + target_table_name
            hive_cursor.execute(dwd_to_tmp)

            # 清空dwd表
            trun_dwd_table = "truncate table " + target_table_schema + "." + target_table_name
            hive_cursor.execute(trun_dwd_table)

            # ods数据移至dwd
            ods_to_dwd = "insert into table " + target_table_schema + "." + target_table_name \
                             + " select *,0 record_is_delete from " + source_table_schema + "." + source_table_name \
                             + " where etl_time = '" + run_period + "'"
            hive_cursor.execute(ods_to_dwd)

            # tmp中不重复数据至dwd
            tmp_to_dwd = "insert into table " + target_table_schema + "." + target_table_name \
                            + " select a.* from " + target_table_schema + "_tmp." + target_table_name + " a " \
                            + " LEFT OUTER JOIN " + target_table_schema + "." + target_table_name + " b " \
                            + " on " + pkey_cond  \
                            + " where b." + pkey + " is null"
            hive_cursor.execute(tmp_to_dwd)

            # update job status to 0 (finish)
            update_job_status = "update hive_etl.etl_conf_job set status = 0, retry_cnt = 0 where id = " + str(job_id)
            self.mysql_db.update(update_job_status)

            time.sleep(20)

            hive_conn.close()
            return 0

        except Exception, e:
            logging.error(str(Exception))
            logging.error(str(e))
            # 回滚
            trun_dwd_table = "truncate table " + target_table_schema + "." + target_table_name
            hive_cursor.execute(trun_dwd_table)
            # rollback tmp to current dwd table
            tmp_rollback_dwd = "insert into " + target_table_schema + "." + target_table_name + " select * from " +target_table_schema + "_tmp." + target_table_name
            hive_cursor.execute(tmp_rollback_dwd)

            if retry_cnt >= max_retry_cnt:
                # update job status to 1(error)
                update_job_status = "update hive_etl.etl_conf_job set status = 1 where id = " + str(job_id)
                self.mysql_db.update(update_job_status)

                hive_conn.close()
                return 1
            else :
                # update job status to 0, update level + 1 to retry
                update_job_status = "update hive_etl.etl_conf_job set status = 0, run_period = '" + run_period + "', retry_cnt = retry_cnt+1 where id = " + str(job_id)
                self.mysql_db.update(update_job_status)

                hive_conn.close()
                return 1

