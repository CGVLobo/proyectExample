# -*- coding: utf-8 -*-
import time
from tools import Tools
from bd_connect import BDConnect
from google.cloud import bigquery
from mailer import mailer


class BQUpdater():
    def __init__(self):

        self.tools_obj = Tools()
        self.printandlog = self.tools_obj.printandlog
        self.remote_config = self.tools_obj.ini('./remote_config.ini')
        self.local_config = self.tools_obj.ini('./config.ini')
        self.__local_conn = None # configurations of the local BD
        self.__remote_conn = None # configurations of the remote BD



    def bigquery_update(self):
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
                    if not self.big_query_save():
                        raise Exception('Exception in big_query_save()')

                    # stop tracking the time
                    end = time.time()
                    rest = end - start

                    # Waiting time - rest
                    print('Esperando '+ self.local_config['wait_time'] + ' segundos.')
                    if rest < float(self.local_config['wait_time']):
                        time.sleep(float(self.local_config['wait_time']) - rest)
                    else:
                        time.sleep(float(self.local_config['wait_time']))
                except Exception as e:
                    self.printandlog('Exception in the loop from bigquery_update(): ' + str(e), 'error.log')
                    counter += 1
                    print('intento' + str(counter))
                    if counter == 10:
                        mailer(self.local_config['emailfrom'], self.local_config['emailto'],
                               self.local_config['password'],
                               self.local_config['server'], self.local_config['port']).send_mail(
                            'Se produjo un error en el bucle de la funcion bigquery_update')
                        break
                    time.sleep(10)


        except Exception as e:
            self.printandlog('Exception in bigquery_update(): ' + str(e), 'error.log')
            if self.local_config['emailfrom']:
                try:
                    mailer(self.local_config['emailfrom'], self.local_config['emailto'],
                           self.local_config['password'],
                           self.local_config['server'], self.local_config['port']).send_mail(
                        'Se produjo un error en funcion bigquery_update')
                except Exception as e:
                    pass


    def big_query_save(self):
        # Get the last row inserted in Big Query
        client = bigquery.Client()


        sql_table = self.get_rows_to_insert()

        if sql_table:
            try:
                insertions_big_query = []
                # Construct a BigQuery client object.
                client = bigquery.Client()

                for row in sql_table:
                    print('Ultima insercion en Big Query: ' + str(row[0]) + ' ' + str(row[1]))
                    insertions_big_query.append(
                        {'hora': row[0], 'fecha': row[1].strftime("%Y-%m-%d"), 'chv': row[2], 'red': row[3],
                         'mega': row[4], 'tvn': row[5], 'c13': row[6], 'ucv': row[7],
                         'tc': row[8], 'cable': row[9], 'hogar': row[10]})


                errors = client.insert_rows_json(self.remote_config['big_query_path'], insertions_big_query)  # Make an API request.
                if errors == []:
                    self.printandlog(str(len(sql_table)) + ' rows inserted in Big Query', 'output.log')
                    return True
                else:
                    self.printandlog("Encountered errors while inserting rows: {}".format(errors), 'error.log')
                    return False
            except Exception as e:
                self.printandlog('Exception in big_query_save(): ' + str(e), 'error.log')
                return False


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

BQUpdater().bigquery_update()
