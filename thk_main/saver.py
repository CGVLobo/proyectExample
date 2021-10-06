# -*- coding: UTF-8 -*-
import time
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import tools
import os
from bd_connect import BDConnect


class Saver:
    '''
    this class contains all the methods needed to save the extract fields into the database
    '''

    def __init__(self):
        '''
        :param conn: the session connection to DB
        '''
        self.__conn = BDConnect().get_connection()

        self.__path = Path(os.path.dirname(os.path.abspath(__file__)) + '/config.ini')
        self.__tools_obj = tools.Tools()
        self.is_time_format = self.__tools_obj.is_time_format
        self.__config_dict = self.__tools_obj.ini(self.__path)
        self.printandlog = self.__tools_obj.printandlog
        self.columns = ['hora', 'chilevision', 'lared', 'mega', 'tvn', 'canal13', 'tvmas',
                        'telecanal']

    def save(self, table, actual_date, last_hour):
        '''
        '''

        try:
            if self.__conn is None:
                self.printandlog('Opening a new connection: ', 'output.log')
                self.__conn = BDConnect().get_connection()
        except Exception as e:
            self.printandlog('Exception trying to open a connection: ' + str(e), 'error.log')

        cursor = None
        try:
            # Converting to time format
            last_hour = time.strptime(last_hour, '%H:%M:%S')

            # for every row
            for count, number_list in enumerate(table):

                '''Begin Transaction'''
                cursor = self.__conn.cursor(buffered=True)
                SqlString = ''

                # Detecting if an hour is from the actual date or the past day
                row_hour = time.strptime(number_list[0]+':00', '%H:%M:%S')
                if row_hour > last_hour:
                    row_date = datetime.strptime(actual_date, "%Y-%m-%d") - timedelta(days=1)
                    row_date = row_date.strftime("%Y-%m-%d")
                else:
                    row_date = actual_date

                SqlStringSelect = """SELECT * FROM """+self.__config_dict['db_table']+""" WHERE hora =%s AND fecha LIKE %s;"""
                cursor.execute(SqlStringSelect, ( number_list[0]+':00', row_date + '%'))
                resultado = cursor.fetchone()

                if not resultado:
                    insert = True
                    for count, value in enumerate(number_list):
                        # If it is hour and it was not correctly extracted, do not insert the row
                        if count == 0 and not self.is_time_format(value):
                            insert = False
                            break
                        # if it is value and it was not correctly extracted, do not insert the row
                        if count != 0 and not str(value).isnumeric():
                            insert = False
                            break
                    if insert:
                        SqlString = """INSERT INTO """+self.__config_dict['db_table']+""" (hora,fecha,chv,red,mega,tvn,c13,ucv,tc,cable,hogar) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

                        data = (number_list[0]+':00',row_date,
                                number_list[1], number_list[2], number_list[3], number_list[4], number_list[5],
                                number_list[6],
                                number_list[7],number_list[8],number_list[9])
                        cursor.execute(SqlString, data)
                        self.__conn.commit()
                cursor.close()

        except Exception as e:
            self.printandlog('Exception while saving: ' + str(e), 'error.log')
            if cursor:
                self.__conn.rollback()
                cursor.close()
                self.__conn.close()
                self.__conn = None








