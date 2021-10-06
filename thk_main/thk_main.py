# -*- coding: utf-8 -*-
import os
import time
from table_parser import TableParser
import pyautogui
import win32gui
from tools import Tools
from saver import Saver
from mailer import mailer
from datetime import datetime
from datetime import timedelta


class THK():
    def __init__(self):
        self.tools_obj = Tools()
        self.is_time_format = self.tools_obj.is_time_format
        self.log = self.tools_obj.printandlog
        self.rootdir = os.path.dirname(os.path.abspath(__file__))
        self.configdict = self.tools_obj.ini(self.rootdir + './config.ini')
        self.tbl_parser = TableParser()
        self.saver = Saver()




    def screenshot(self, window_title=None):
        if window_title:
            hwnd = win32gui.FindWindow(None, window_title)
            if hwnd:
                win32gui.SetForegroundWindow(hwnd)
                print('maximizando ventana con espera de '+self.configdict['wait_maximization']+' segundos')
                time.sleep(float(self.configdict['wait_maximization'])) # wait for bringing the window to foreground
                x, y, x1, y1 = win32gui.GetClientRect(hwnd)
                x, y = win32gui.ClientToScreen(hwnd, (x, y))
                x1, y1 = win32gui.ClientToScreen(hwnd, (x1 - x, y1 - y))
                im = pyautogui.screenshot(region=(x, y, x1, y1))
                print(x,y,x1,y1)
                return im
            else:
                self.log('Window not found!', './error.log')

        else:
            im = pyautogui.screenshot()
            return im

    def process_table(self):
        try:

            #Taking screenshot
            self.log('Taking screenshot', './output.log')
            capture = self.screenshot(self.configdict['program_name'])

            # Getting the actual date and time
            actual_date = time.strftime("%Y-%m-%d")
            actual_time = time.strftime('%H:%M')
            actual_time = str(actual_time) +':00'
            actual_time = datetime.strptime(actual_time, '%H:%M:%S')

            # Saving Image
            capture.save(self.configdict['image_path'])

            # Getting the values of the table
            self.log('Parsing values', './output.log')
            table = self.tbl_parser.get_table_values(self.log)

            # If table has something, save it
            if table:
                # If the last hour of the table was correctly extracted
                last_time = table[-1][0]

                if self.is_time_format(last_time):
                    # if the last time of the table is bigger than the actual time of the PC
                    # (ie. 23:59 > 00:00) then, the current date (actual_date) must be yesterday
                    # Formating the last row time
                    last_time = last_time +':00'
                    last_time = datetime.strptime(last_time, '%H:%M:%S')
                    if last_time > actual_time:
                        actual_date = datetime.strptime(actual_date, "%Y-%m-%d") - timedelta(days=1)
                        actual_date = actual_date.strftime("%Y-%m-%d")
                    # Reconverting the last row time to string
                    last_time = datetime.strftime(last_time, '%H:%M:%S')
                    # Saving in mysql
                    self.saver.save(table, actual_date, last_time)

        except Exception as e:
            self.log('error in process_table: ' + str(e), './error.log')

    def thk_execute(self):
        try:

            # Process the table every 20 seconds
            while (True):
                self.log('procesando tabla', './output.log')
                self.process_table()

                print('Esperando '+self.configdict['wait_time']+' seg.')
                time.sleep(float(self.configdict['wait_time']))
        except Exception as e:
            self.log('error in thk_execute: ' + str(e), './error.log')
            if self.configdict['emailfrom']:
                try:
                    mailer(self.configdict['emailfrom'], self.configdict['emailto'],
                           self.configdict['password'],
                           self.configdict['server'], self.configdict['port']).send_mail(
                        'Se produjo una excepcion en la funcion de thk_ocr ')
                except Exception as e:
                    pass



THK().thk_execute()


