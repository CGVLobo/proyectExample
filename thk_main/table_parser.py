# Import required packages
# pip install opencv-contrib-python
import cv2
import pytesseract
import time
from configparser import  ConfigParser
import os


class TableParser:
    def __init__(self):
        # Mention the installed location of Tesseract-OCR in your system
        self.__parser = ConfigParser()
        self.__parser.read('./config.ini')
        pytesseract.pytesseract.tesseract_cmd = self.__parser.get('config', 'tesseract_path')

    def get_table_values(self, printandlog):
        try:
            try:

                # Read image from which text needs to be extracted
                img = cv2.imread(self.__parser.get('config', 'image_path'))

                (H, W) = img.shape[:2]

                # set the new width and height and then determine the ratio in change
                # for both the width and height
                (newW, newH) = (900, 520)

                # resize the image and grab the new image dimensions
                headless_first_table = cv2.resize(img, (newW, newH))
                (H, W) = headless_first_table.shape[:2]

                # # Extracting the part from the table with Rating and Share
                # headless_second_table = headless_first_table[405:480, 90:887]
                # headless_second_table = cv2.resize(headless_second_table, (900, 104))

                # Extracting the rest of the table
                headless_first_table = headless_first_table[90:395, 90:887]
                headless_first_table = cv2.resize(headless_first_table, (newW, newH))


            except Exception as e:
                printandlog('get_table_values(): error cutting the image: ' + str(e), './error.log')
                return None

            # Processing the table values
            im_height, im_width = headless_first_table.shape[:2]
            table_values = []
            h = 52
            y = 0
            while (y < im_height):

                fila = []
                row = headless_first_table[y:y + h, 0:im_width]
                y = y+h
                hora = row[:, 10:80]
                # making gray
                hora = cv2.cvtColor(hora, cv2.COLOR_BGR2GRAY)

                # Resize hora
                hora = cv2.resize(hora, (145, 60))

                # Performing OTSU threshold
                hora = cv2.threshold(hora, 0, 255, cv2.THRESH_OTSU | cv2.THRESH_BINARY_INV)[1]


                valores = row[:, 85:im_width]


                text = pytesseract.image_to_string(hora,config='-c tessedit_char_whitelist=0123456789: --psm 7')
                fila.append(text.replace('\n\x0c','')) #TODO ver esto, por que agrega esos caracteres


                for i in range(10):
                    # If it is the column number 8, dont include
                    if i == 8:
                        continue

                    valor = valores[:, i*82:(i+1)*82]
                    # making a copy in gray
                    valor = cv2.cvtColor(valor, cv2.COLOR_BGR2GRAY)
                    # Resize to extend it horizontally
                    valor = cv2.resize(valor, (145, 60))
                    # Performing OTSU threshold
                    valor = cv2.threshold(valor, 0, 255, cv2.THRESH_OTSU | cv2.THRESH_BINARY_INV)[1]


                    text2 = pytesseract.image_to_string(valor,config='-c tessedit_char_whitelist=0123456789: --psm 7')
                    text2 = text2.replace('{','1') # TODO no registra bien el 1
                    fila.append(text2.replace('\n\x0c','')) #TODO ver esto, por que agrega esos caracteres
                table_values.append(fila)
                if len(table_values) ==1:
                    actual_date = time.strftime("%Y-%m-%d")

                    if (self.__parser.get('config', 'last_time') != table_values[0][0] or self.__parser.get('config', 'last_date')!= actual_date):
                        # Save the actual date and actual THK hour
                        self.__parser.set('config', 'last_time', table_values[0][0])
                        self.__parser.set('config', 'last_date', actual_date)
                    else:
                        print("Tabla con valores ya guardada")
                        return None


                    with open('config.ini', 'w') as configfile:
                        self.__parser.write(configfile)


            return table_values
        except Exception as e:
            printandlog('error in get_table_values: ' + str(e), './error.log')
            return None






