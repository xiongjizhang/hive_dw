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


class DwJobOds:
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
        # dt = datetime.datetime.strptime(self.job_info[10],'%Y%m%d') + datetime.timedelta(days=1)
        # run_period = dt.strftime('%Y%m%d')

        job_id = self.job_info['id']
        source_conn_id = self.job_info['source_conn_id']
        check_file_name = self.job_info['source_info1'].replace("${RUNPERIOD}", run_period)
        data_file_name = self.job_info['source_info2'].replace("${RUNPERIOD}", run_period)
        target_conn_id = self.job_info['target_conn_id']
        table_schema = self.job_info['target_info1']
        table_name = self.job_info['target_info2']
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

        # download file
        target_path = '/home/hadoop/code/python/hive_dw_etl/temp/data/'
        sf = paramiko.Transport((sftp_host,sftp_port))
        sf.connect(username = sftp_name, password = sftp_pw)
        sftp = paramiko.SFTPClient.from_transport(sf)
        try:
            sftp.get(source_path+check_file_name, target_path+check_file_name)
            sftp.get(source_path+data_file_name, target_path+data_file_name)
            logging.info('finish download file!')
            sf.close()
        except:
            logging.warning('download file failure!')
            if retry_cnt >= max_retry_cnt:
                # update job status to 2(not file)
                update_job_status = "update hive_etl.etl_conf_job set status = 2, retry_cnt = retry_cnt + 1 where id = " + str(job_id)
                self.mysql_db.update(update_job_status)
                return 1
            else :
                # update job status to 0, update level + 1 to retry
                update_job_status = "update hive_etl.etl_conf_job set run_period = '" + last_runperiod + "', status = 0, retry_cnt = retry_cnt +1 where id = " + str(job_id)
                self.mysql_db.update(update_job_status)
                return 1

        copy_from_local_cmd = "hadoop fs -put -f " + target_path + data_file_name  + " hdfs://" + hive_host  + ":9000/data/"
        logging.info(copy_from_local_cmd)
        os.system(copy_from_local_cmd)
        logging.info("Continue!!!!!")
        load_data_sql = "LOAD DATA INPATH '"+ "hdfs://" + hive_host  + ":9000/data/" + data_file_name +"' OVERWRITE INTO TABLE ods." + table_name + " PARTITION (etl_time='"+ run_period +"')"
        logging.info(load_data_sql)

        try :
            hive_conn = pyhs2.connect(host=hive_host, port=hive_port, authMechanism=hive_authmechanism, user=hive_user, password=hive_pw, database=table_schema)
            # host主机ip,port：端口号，username:用户名，database:使用的数据库名称
            hive_cursor=hive_conn.cursor()
            hive_cursor.execute(load_data_sql) # 执行查询

            # update job status to 0 (finish)
            update_job_status = "update hive_etl.etl_conf_job set status = 0, retry_cnt = 0 where id = " + str(job_id)
            self.mysql_db.update(update_job_status)

            # time.sleep(20)

            hive_conn.close()
            return 0

        except Exception, e:
            logging.error(str(Exception))
            logging.error(str(e))
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

