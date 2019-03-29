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


class DwJobOds:
    def __init__(self,job_info):
        self.info = job_info
        self.etl_db_conf = ReadConf('../conf/etl_db_mysql.conf')
        self.mysql_db = MySql(self.etl_db_conf)

    def run(self):
        # 获取作业的相关配置信息
        dt = datetime.datetime.strptime(self.info[7],'%Y%m%d') + datetime.timedelta(days=1)
        run_period = dt.strftime('%Y%m%d')

        data_file_name = self.info[2].replace("${YESTERDAY}",run_period)
        check_file_name = self.info[3].replace("${YESTERDAY}",run_period)
        table_name = self.info[5].replace("_${YESTERDAY}","")
        job_id = self.info[0]

        etl_db_conf = ReadConf('../conf/etl_db_mysql.conf')
        mysql_db = MySql(self.etl_db_conf)

        source_conn_sql = "select a.* \
                           from hive_etl.etl_conf_conn_account a \
                           left join hive_etl.etl_conf_job b on a.id = b.source_conn_id \
                           where b.id = " + job_id
        source_conn = self.mysql_db.query(source_conn_sql)

        sftp_host = source_conn[3]
        sftp_port = source_conn[4]
        sftp_name = source_conn[1]
        sftp_pw = source_conn[2]
        source_path = source_conn[5]

        obj = AES.new('This is a key114'.encode('utf-8'), AES.MODE_CBC, 'This is an WZ456'.encode('utf-8'))
        sftp_pw = obj.decrypt(sftp_pw).rstrip('\0')  # obj.encrypt('123456'.encode('utf-8'))

        target_conn_sql = "select a.* \
                           from hive_etl.etl_conf_conn_account a \
                           left join hive_etl.etl_conf_job b on a.id = b.target_conn_id \
                           where b.id = " + job_id
        target_conn = self.mysql_db.query(target_conn_sql)

        hive_host = target_conn[3]
        hive_port = target_conn[4]
        hive_user = target_conn[1]
        hive_pw = target_conn[2]
        hive_authmechanism = target_conn[5]

        obj = AES.new('This is a key114'.encode('utf-8'), AES.MODE_CBC, 'This is an WZ456'.encode('utf-8'))
        hive_pw = obj.decrypt(hive_pw).rstrip('\0')

        # download file
        target_path = 'temp/data/'
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
            if self.info[10] >= 5:
                # update job status to 2(not file)
                update_job_status = "update etl.etl_conf_job set status = 2, level = level + 1 where id = " + str(job_id)
                self.mysql_db.update(update_job_status)
                return 1
            else :
                # update job status to 0, update level + 1 to retry
                update_job_status = "update etl.etl_conf_job set update_date = " + self.info[7] + ", status = 0, level = level +1 where id = " + str(job_id)
                self.mysql_db.update(update_job_status)
                return 0

        copy_from_local_cmd = "hadoop fs -put -f " + target_path + data_file_name  + " hdfs://" + hive_host  + ":9000/data/"
        os.system(copy_from_local_cmd)
        load_data_sql = "LOAD DATA INPATH '"+ "hdfs://" + hive_host  + ":9000/data/" + data_file_name +"' OVERWRITE INTO TABLE ods." + table_name + " PARTITION (etl_time='"+ run_period +"')"

        try :
            hive_conn = pyhs2.connect(host=hive_host, port=hive_port, authMechanism=hive_authmechanism, user=hive_user, password=hive_pw, database=self.info[7])
            # host主机ip,port：端口号，username:用户名，database:使用的数据库名称
            hive_cursor=hive_conn.cursor()
            hive_cursor.execute(load_data_sql) # 执行查询

            # update job status to 0 (finish)
            update_job_status = "update etl.etl_conf_job set status = 0, level = 1 where id = " + str(data[0])
            self.mysql_db.update(update_job_status)

            hive_conn.close()
            return 0

        except Exception, e:
            logging.error(str(Exception))
            logging.error(str(e))
            if self.info[10] > 5:
                # update job status to 1(error)
                update_job_status = "update etl.etl_conf_job set status = 1 where id = " + str(data[0])
                self.mysql_db.update(update_job_status)

                hive_conn.close()
                return 1
            else :
                # update job status to 0, update level + 1 to retry
                update_job_status = "update etl.etl_conf_job set status = 0, update_date = date_format(subdate(update_date,1),'%Y%m%d'), level = level+1 where id = " + str(job_id)
                self.mysql_db.update(update_job_status)

                hive_conn.close()
                return 1

