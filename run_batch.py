#!/usr/bin/env python
#coding=utf-8

import sys,os
import logging

from util.base.args_format import ArgsFormat
from dw_service.core.dw_service_batch import DwBatchCore

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

argsFormat = ArgsFormat(sys.argv)
#需要解析的长参数
argsFormat.setlongOption(["batch_id=", "batch_code=","batch_name="])
#需要解析的段参数
#argsFormat.setshortOption("m:f:")
argsMap = argsFormat.run()

# 初始化
batch = DwBatchCore(argsMap['--batch_id'])
batch.start()


