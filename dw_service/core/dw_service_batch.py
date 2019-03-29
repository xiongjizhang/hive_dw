#coding=utf-8

from dw_service.core.job.jb_ftp_to_hive import DwJobOds
from dw_service.core.job.dwd.jb_ods_dwd_incr_by_key import JobDwdIncrByKey
from dw_service.core.job.dwd.jb_ods_dwd_incr_no_key import JobDwdIncrNoKey
from util.mysql.mysql import MySql
from util.base.read_conf import ReadConf
import time
import logging

class DwBatchCore:

    u'''
    初始化函数，需要指定批次id
    @par batch_id: 批次id
    @result bool
    '''
    def __init__(self, batch_id):
        self.batch_id = batch_id
        self.etl_db_conf = ReadConf('dw_service/conf/etl_db_mysql.conf')
        self.mysql_db = MySql(self.etl_db_conf.get_conf())
        self.mysql_db.query("select * from hive_etl.etl_conf_batch where id = " + str(batch_id))
        self.batch_info = self.mysql_db.fetchOneRow()
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


    u'''
    判断该批次是否还有作业需要运行
    @result bool: 是否还要需要运行的作业
    '''
    def have_job_to_run(self):
        sql_str = "select count(1) to_run_cnt \
                    from hive_etl.etl_conf_job a \
                    join hive_etl.etl_conf_batch b on a.batch_id = b.id and a.run_period = b.last_period \
                    where a.status = 0 and b.id = " + self.batch_id

        self.mysql_db.query(sql_str)

        return self.mysql_db.fetchOneRow()['to_run_cnt'] > 0


    u'''
    获取下一次将要运行的作业id
    @result integer: 将要运行的作业id
    '''
    def next_job_id(self):
        sql_str = "select a.id \
                    from hive_etl.etl_conf_job a \
                    join hive_etl.etl_conf_batch b on a.batch_id = b.id \
                    left join hive_etl.etl_conf_precondition p on a.id = p.job_id and p.`status` = 0 \
                    left join hive_etl.etl_conf_job c on p.pre_job_id = c.id \
                    where a.run_period = b.last_period and a.status = 0 and a.batch_id = %s \
                    group by a.id \
                    having count(1) = count(if(b.run_period <= ifnull(c.run_period,'99991231'),1,null)) \
                    order by a.retry_cnt, a.priority " % (self.batch_id)
        self.mysql_db.query(sql_str)
        row = self.mysql_db.fetchOneRow()
        if row is None:
            return None
        else :
            return row['id']


    u'''
    更新作业的状态为正常执行完成
    '''
    def update_job_success(self, job_id):
        update_sql = "update hive_etl.etl_conf_job set status = 0, \
                      run_period = '%s',retry_cnt = 0 \
                      where id = %s" % (self.batch_info['run_period']) % job_id
        self.mysql_db.update(update_sql)


    u'''
    当批次的作业完成之后，更新批次的账期
    '''
    def update_batch_period(self) :
        if self.batch_info['interval'] == "S" : # 准实时批次账期更新
            update_sql = "update hive_etl.etl_conf_batch \
                          set run_period = '%s', last_period = run_period \
                          where id = %s" % (time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))) % (self.batch_info['id'])
            self.mysql_db.update(update_sql)
        elif self.batch_info['interval'] == "D" : # 按天批次账期更新
            logging.info(self.batch_info['id'])
            update_sql = "update hive_etl.etl_conf_batch \
                          set last_period = run_period, run_period = date_format(adddate(run_period,1),'%%Y%%m%%d') \
                          where id = %s" % (self.batch_info['id'])
            logging.info(update_sql)
            self.mysql_db.update(update_sql)
        elif self.batch_info['interval'] == "M" : # 按月批次账期更新
            update_sql = "update hive_etl.etl_conf_batch \
                          set run_period = date_format(date_add(concat(run_period, '01'), interval 1 month),'%%Y%%m'), last_period = run_period \
                          where id = %s" % (self.batch_info['id'])
            self.mysql_db.update(update_sql)


    u'''
    开始循环执行批次中的各个作业
    '''
    def start(self):
        max_run_minute = self.batch_info['max_run_minute']
        no_job_sleep = self.batch_info['no_job_sleep']
        start_time = time.time()
        while (time.time() - start_time) < max_run_minute*60 and self.have_job_to_run():
            logging.info("Getting the job id")
            job_id = self.next_job_id()
            if job_id is None:
                logging.info("No job to run! Wait and continue try!!!")
                time.sleep(no_job_sleep)
            else :
                self.mysql_db.query("select * from hive_etl.etl_conf_job where id = " + str(job_id))
                job_info = self.mysql_db.fetchOneRow()

                logging.info("Start Job for id = " + str(job_id) + "!!!")
                # 更新作业的状态为正在运行（9）
                update_job_status = "update hive_etl.etl_conf_job set status = 9, \
                                     run_period = '" + self.batch_info['run_period'] + "' \
                                     where id = " + str(job_info['id'])
                self.mysql_db.update(update_job_status)

                job = eval(job_info['job_class_name'] + '(job_info, self.batch_info)')  # DwJobOds(job_info, self.batch_info)
                run_result = job.run()

                logging.info("Job run result is " + str(run_result))
                if run_result > 0 :
                    time.sleep(no_job_sleep)

        if self.have_job_to_run() == False:
            self.update_batch_period()
