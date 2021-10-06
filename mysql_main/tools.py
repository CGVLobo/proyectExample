# -*- coding: UTF-8 -*-

import time
import re
import configparser
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from imagetyperzapi3.imagetyperzapi import ImageTyperzAPI
import unicodedata
import requests


class Tools:

    def log(self, value, filename, timeban=None):
        '''
            Este metodo sirve para crear un archivo log.txt
        :param value: El texto a guardar en la linea
        :param filename: Nombre absoluto o relativo del nombre del archivo log
        :param timeban: si timeban != None entonces no guarda el tiempo en que se producio el error

        '''
        try:
            if timeban == None:
                import time
                fecha = time.strftime("%d/%m/%y")
                hora = time.strftime("%H:%M:%S")
                text = "%s - %s; %s\n" % (fecha, hora, value)
                with open("%s" % filename, "a", encoding='utf-8') as archivo:
                    archivo.writelines(text)
            else:
                with open("%s" % filename, "a", encoding='utf-8') as archivo:
                    archivo.writelines("%s\n" % value)
        except Exception as e:
            print("Error in log function, ERROR: %s" % e)
            raise

    def ini(self, FileNamePath):
        '''
            Este metodo me permite cargar un listado de variables que se encuentran en un archivo config.ini.

        :param FileNamePath: es el path absoluto o relativo donde se encuentra el archivo config.ini
        :return: ConfigDict: Es un diccionario cuyas keys y values son las variables y sus valores del config.ini
                             correspondientemente. No se discriminar por Section del config.
        '''

        ConfigDict = {}
        try:

            config = configparser.RawConfigParser()
            config.read(FileNamePath, encoding="utf-8")
            for section in config.sections():
                for var in config[section]:
                    ConfigDict[var] = config[section][var]
            return ConfigDict
        except Exception as e:

            texto = ("Error found in ini function, ERROR:%s " % e)
            print(texto)
            self.log(texto, "log.txt", 1)
            raise


    def printandlog(self, text, filename):
        try:
            print(text)
            # save log
            self.log(text, filename, None)
        except Exception as e:
            # save log
            self.log(text, filename, None)
            raise