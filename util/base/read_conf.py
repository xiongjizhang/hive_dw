#coding=utf-8

import ConfigParser
from Crypto.Cipher import AES

class ReadConf:

    def __init__(self, conf_path):
        self.conf_path = conf_path
        self.conf_dist = {}
        self.read_conf()

    def read_conf(self):
        cf = ConfigParser.ConfigParser()
        cf.read(self.conf_path)
        kvs = cf.items("etl_conf")

        conf_dist = {}
        for (k,v) in kvs:
            if k == 'port' :
                conf_dist[k] = int(v)
            elif k == 'passwd' :
                obj = AES.new('This is a key114'.encode('utf-8'), AES.MODE_CBC, 'This is an WZ456'.encode('utf-8'))
                v = obj.decrypt(v.decode("string-escape")).rstrip('\0')  # obj.encrypt('123456'.encode('utf-8'))
                conf_dist[k] = v
            else :
                conf_dist[k] = v

        self.conf_dist = conf_dist

    def get_conf(self):
        return self.conf_dist