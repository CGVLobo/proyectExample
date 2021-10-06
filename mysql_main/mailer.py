import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

class mailer():
    def __init__(self, emailfrom, emailto, password, server, port):
        '''
        This is the class constructor that takes all the parameters to send the email.
        :param emailfrom: From address.
        :param emailto: To address.
        :param password: Password to authenticate on the server.
        :param server: Server to use to send the email.
        :param port: Port to use.
        :param message: Message to send.
        :param sleep: Time to sleep after sending the email.
        '''
        self.__emailfrom = emailfrom
        emailto = str(emailto).split(',')
        self.__emailto = emailto
        self.__password = password
        self.__server = server
        self.__port = port
        self.__context = ssl.create_default_context()

    def send_mail(self, message):
        '''
        The method that sends the email.
        :param message: Message to be sent.
        :return:
        '''
        self.__message = message
        server = smtplib.SMTP(self.__server, self.__port)
        server.starttls(context=self.__context)
        server.login(self.__emailfrom, self.__password)
        print("Sending email to {}".format(self.__emailto))
        server.sendmail(self.__emailfrom, self.__emailto, self.__message)
        server.quit()

    def sendMailWithSubject(self, message, subject):
        '''
        :param subject: Subject for the email.
        :param message: Message to be sent.
        '''
        msg = MIMEMultipart('related')
        msg['Subject'] = subject
        msg['From'] = self.__emailfrom
        msg['To'] = self.__emailto

        html = """\
                <html>
                  <head></head>
                    <body>
                    <p><h4 style="font-size:15px;">{}</h4></p>
                    </body>
                </html>
                """.format(message)
        # Record the MIME types of text/html.
        part2 = MIMEText(html, 'html')

        # Attach parts into message container.
        msg.attach(part2)

        server = smtplib.SMTP(self.__server, self.__port)
        server.starttls(context=self.__context)
        server.login(self.__emailfrom, self.__password)
        server.sendmail(self.__emailfrom, self.__emailto, msg.as_string())
        server.quit()

    def sendImageMail(self, subject, image, href):
        '''
        :param subject: Subject for the email.
        :param image: Path to the imege to be sent.
        :param href: Link that will be attached to the image.
        :return:
        '''

        self.__subject = subject
        self.__image = image
        self.__href = href

        msg = MIMEMultipart('related')
        msg['Subject'] = self.__subject
        msg['From'] = self.__emailfrom
        msg['To'] = self.__emailto

        html = """\
                <html>
                  <head></head>
                    <body>
                        <a href={} ><img src="cid:image1" ><br></a>
                       <p><h4 style="font-size:15px;"></h4></p>
                    </body>
                </html>
                """.format(self.__href)
        # Record the MIME types of text/html.
        part2 = MIMEText(html, 'html')

        # Attach parts into message container.
        msg.attach(part2)

        # This example assumes the image is in the current directory
        fp = open(self.__image, 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close()

        # Define the image's ID as referenced above
        msgImage.add_header('Content-ID', '<image1>')
        msg.attach(msgImage)

        # Send the message via local SMTP server.
        server = smtplib.SMTP(self.__server, self.__port)
        server.starttls(context=self.__context)
        server.login(self.__emailfrom, self.__password)
        server.sendmail(self.__emailfrom, self.__emailto, msg.as_string())
        server.quit()
