# -*- coding: utf-8 -*-

import mysql.connector
from tools import Tools
import os


class BDConnect:

    def __init__(self, config_path):
        self.tools_obj = Tools()
        self.rootdir = os.path.dirname(os.path.abspath(__file__))
        self.configdict = self.tools_obj.ini(self.rootdir + config_path)


    def get_connection(self):
        try:
            conn = mysql.connector.connect(host=self.configdict['db_host'],
                                           database=self.configdict['db_name'],
                                           password=self.configdict['db_password'],
                                           user=self.configdict['db_user'],
                                           port=self.configdict['db_port'])
            return conn
        except Exception as e:
            print('::BDConection:: cant connect to DB Exception: {}'.format(e))
            raise
