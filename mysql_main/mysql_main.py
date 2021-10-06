# -*- coding: utf-8 -*-
import time
from tools import Tools
from bd_connect import BDConnect
from google.cloud import bigquery
from mailer import mailer


class MySqlUpdater():
    def __init__(self):

        self.tools_obj = Tools()
        self.printandlog = self.tools_obj.printandlog
        self.remote_config = self.tools_obj.ini('./remote_config.ini')
        self.local_config = self.tools_obj.ini('./config.ini')
        self.__local_conn = None # configurations of the local BD
        self.__remote_conn = None # configurations of the remote BD



    def mysql_update(self):
        '''
        This function updates the rows of rating from the local BD to the remote mysql and bigquery
        :return:
        '''
        try:
            counter = 0

            # infinite while that executes passing one hour
            while (True):
                try:

                    # begin the time tracking
                    start = time.time()

                    # Inserting into Big Query
                    self.remote_mysql_save()

                    # stop tracking the time
                    end = time.time()
                    rest = end - start

                    # Waiting time - rest
                    print('Esperando '+ self.local_config['wait_time'] + ' segundos.' )
                    if rest < float(self.local_config['wait_time']):
                        time.sleep(float(self.local_config['wait_time']) - rest)
                    else:
                        time.sleep(float(self.local_config['wait_time']))
                except Exception as e:
                    self.printandlog('Exception in the loop from mysql_update(): ' + str(e), 'error.log')
                    counter += 1
                    print('intento' + str(counter))
                    if counter == 10:
                        mailer(self.local_config['emailfrom'], self.local_config['emailto'],
                               self.local_config['password'],
                               self.local_config['server'], self.local_config['port']).send_mail(
                            'Se produjo un error en la funcion de mysql_main')
                        break
                    time.sleep(10)


        except Exception as e:
            self.printandlog('Exception in mysql_update(): ' + str(e), 'error.log')
            if self.local_config['emailfrom']:
                try:
                    mailer(self.local_config['emailfrom'], self.local_config['emailto'],
                           self.local_config['password'],
                           self.local_config['server'], self.local_config['port']).send_mail(
                        'Se produjo un error en la funcion de mysql_main')
                except Exception as e:
                    pass


    def remote_mysql_save(self):

        try:
            # Reopen a remote connection if it was closed
            self.__remote_conn = self.check_for_new_connection(self.__remote_conn, './remote_config.ini')
            remote_cursor = None
            remote_cursor = self.__remote_conn.cursor(buffered=True)
            # Execute query to get the rows from the local BD
            sql_table = self.get_rows_to_insert()
            # if there is results
            if sql_table:
                # Inserting into remote mysql BD
                insertions_sql = []

                SqlString = """INSERT INTO """+self.remote_config['db_table']+""" (hora,fecha,chv,red,mega,tvn,c13,ucv,tc,cable,hogar) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                 ON DUPLICATE KEY UPDATE tc=VALUES(tc);"""

                # Iterating the rows to get appropiate structures to save in mysql and bigquery
                for row in sql_table:
                    print('Ultima insercion en mysql: ' + str(row[0]) + ' ' +row[1].strftime("%Y-%m-%d"))
                    insertions_sql.append((row[0], row[1].strftime("%Y-%m-%d"), row[2], row[3], row[4],
                                           row[5], row[6], row[7], row[8], row[9],
                                           row[10]))

                # Inserting into remote BD
                remote_cursor.executemany(SqlString, insertions_sql)
                self.__remote_conn.commit()
                self.printandlog(str(len(sql_table)) + ' rows inserted in MySql', 'output.log')
            remote_cursor.close()
            self.__remote_conn.close()
            self.__remote_conn = None
        except Exception as e:
            if self.__remote_conn:
                self.__remote_conn.close()
                self.__remote_conn = None
            self.printandlog('Exception in remote_mysql_save with: ' +str(e), 'error.log')
            raise e

    def check_for_new_connection(self, conn, configpath):
        '''

        :param conn: the actual connection
        :param configpath: the path to the configuration file with the connection data
        :return: the old or new connection, it depends if the old conn was None
        '''
        try:
            if conn is None:
                conn = BDConnect(configpath).get_connection()
                return conn
            return conn
        except Exception as e:
            self.printandlog('Exception in check_for_new_connection with: ' +configpath+' '+ str(e), 'error.log')



    def get_rows_to_insert(self):
        try:
            # Reopen a local connection if it was closed
            self.__local_conn = self.check_for_new_connection(self.__local_conn, './config.ini')
            local_cursor = None
            local_cursor = self.__local_conn.cursor(buffered=True)
            SqlString = """SELECT fecha, hora FROM """ + self.local_config['db_table'] + """ order by concat(fecha, ' ',hora) DESC limit 1;"""
            local_cursor.execute(SqlString)
            resultado = local_cursor.fetchone()
            last_date = resultado[0]
            last_time = resultado[1]
            date_param = '{} {}'.format(last_date, last_time)
            SqlStringSelect = """SELECT * FROM """+self.local_config['db_table']+""" WHERE CONCAT(fecha,' ',hora) <= DATE_SUB('"""+date_param+"""', INTERVAL 1 HOUR) order by concat(fecha, ' ',hora) DESC limit 1;"""
            local_cursor.execute(SqlStringSelect,)
            sql_table = local_cursor.fetchall()
            local_cursor.close()
            self.__local_conn.close()
            self.__local_conn= None
            return sql_table
        except Exception as e:
            self.printandlog('Exception in get_rows_to_insert: '+ str(e), 'error.log')
            if local_cursor:
                self.__local_conn.rollback()
                local_cursor.close()
                self.__local_conn.close()
                self.__local_conn = None

MySqlUpdater().mysql_update()
