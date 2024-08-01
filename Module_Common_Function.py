import shutil
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
import logging
from pathlib import Path
import math
import sqlalchemy
import pandas as pd
import base64
import datetime
import pymysql
import comtypes.client
import win32com.client as win32
import os


# define list to save year
list_year_yyyy = [str(i) for i in range(2023, 2124, 1)]

# define list to save month
list_english_month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
list_numerical_month = [str(i).rjust(2, "0") for i in range(1, 13, 1)]
dict_english_numerical_month = dict(zip(list_english_month, list_numerical_month))

# define list to save 26 english letters
list_letters_upper = [chr(i) for i in range(65, 91, 1)]
list_letters_lower = [chr(i).lower() for i in range(65, 91, 1)]

# define list to save year month
list_year_month = []
for year in list_year_yyyy:
    for month in list_numerical_month:
        year_month = "".join([year, month])
        list_year_month.append(year_month)

# suffix of files
excel_suffix = [".xlsm", ".xlsx", ".xlsb", ".xls"]
picture_suffix = [".png", ".jpeg", "", ".jpg", ".gif", ".svg"]
pdf_suffix = [".pdf"]


# define one object to send email
class SendEmail:
    """
    One object to send email
    """

    def __init__(self, sender_name, sender_address, receiver, cc, subject, content):
        """
        :param sender_name:  str | sender_name,
        :param sender_address:  str | sender_address,
        :param receiver: list | email address of receiver,
        :param cc: list | email address of Cc,
        :param subject: str | email title
        :param content: str | content of email, can only be normal text
        """
        self.sender_name = sender_name
        self.sender_address = sender_address
        self.receiver = receiver
        self.cc = cc
        self.subject = subject
        self.content = content

    def send_email_with_text(self):
        # create connection to Email Serve
        email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

        # create email object
        msg = MIMEMultipart()

        # create subject
        title = Header(s=self.subject, charset="utf-8").encode()
        msg["Subject"] = title

        # set sender
        msg["From"] = formataddr((self.sender_name, self.sender_address))

        # set receiver
        msg["To"] = ",".join(self.receiver)

        # set Cc
        msg["Cc"] = ",".join(self.cc)

        # add content
        text = MIMEText(_text=self.content, _subtype="plain", _charset="utf-8")
        msg.attach(text)

        # extend receiver list
        to_list = msg["To"].split(",")
        cc_list = msg["Cc"].split(",")
        to_list.extend(cc_list)

        # send email
        email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
        email_server.quit()

    def send_email_with_html(self):
        # create connection to Email Serve
        email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

        # create email object
        msg = MIMEMultipart()

        # create subject
        title = Header(s=self.subject, charset="utf-8").encode()
        msg["Subject"] = title

        # set sender
        msg["From"] = formataddr((self.sender_name, self.sender_address))

        # set receiver
        msg["To"] = ",".join(self.receiver)

        # set Cc
        msg["Cc"] = ",".join(self.cc)

        # add content
        html = MIMEText(_text=self.content, _subtype="html", _charset="utf-8")
        msg.attach(html)

        # extend receiver list
        to_list = msg["To"].split(",")
        cc_list = msg["Cc"].split(",")
        to_list.extend(cc_list)

        # send email
        email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
        email_server.quit()


class Logger:
    """
    define one logger for common usage
    """

    def __init__(self, level, file_name):
        """
        :param level: logging level
        :param file_name: absolute directory of log file
        """

        self.level = level
        self.file_name = file_name

    def basic_configuration(self):
        """
        :return: basic configuration for logging
        """
        return logging.basicConfig(level=self.level,
                                   filename=self.file_name,
                                   filemode="w",
                                   format="%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")


class MSSQL:
    """
    define one object to get connection with MS SQL Server
    """

    def __init__(self, server, user, password, database):
        """
        :param server: str | host name of MS SQL Server
        :param user: str | user to log in MS SQL Server
        :param password: str | password to log in MS SQL Server
        :param database: str | database name in MS SQL Server
        """
        self.server = server
        self.user = user
        self.database = database
        self.password = password

    def pyodbc_connection(self):
        """
        :return: pyodbc connection
        """
        if not self.database:
            raise (NameError, "Incorrect configuration of MS SQL Server !")

        # create connection to SQL Server
        connection_string = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password}'
        try:
            pyodbc_con = pyodbc.connect(
                connection_string,
                fast_executemany=True
            )

            return pyodbc_con

        except Exception:
            raise Exception("Failed to connect to MS SQL Server !")

    def sqlalchemy_connection(self):
        """
        :return: SQLAlchemy + pyodbc connection
        """

        # create connection to SQL Server
        try:
            engine = sqlalchemy.create_engine(
                'mssql+pyodbc://{}:{}@{}/{}?driver=ODBC+Driver+17+for+SQL+Server'.format(self.user,
                                                                                         self.password,
                                                                                         self.server,
                                                                                         self.database),
                fast_executemany=True)

            sqlalchemy_con = engine.connect()

            return sqlalchemy_con

        except Exception:
            raise Exception("Failed to connect to MS SQL Server !")

    def add_table_property(self, table_name, table_desc):
        """
        :param table_name: table name in MS SQL Server
        :return:
        """

        # create sql string
        sql = f"""
        EXEC sp_addextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql string
        cursor.execute(sql)

        # submit and close
        con.commit()
        con.close()

    def update_table_property(self, table_name, table_desc):
        """
        :param table_name: table name in MS SQL Server
        :return:
        """

        # create sql string
        sql = f"""
        EXEC sp_updateextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql string
        cursor.execute(sql)

        # submit and close
        con.commit()
        con.close()

    def execute_sql_query(self, sql):
        """
        :param sql: sql query string
        :return: None
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql query
        cursor.execute(sql)

        # commit and close
        con.commit()
        con.close()


class PDFData:
    """
    Deal with PDF data
    """

    def __init__(self, dir_pdf):
        """
        :param dir_pdf: absolute directory of PDF file
        """

        self.dir_pdf = dir_pdf

    def convert_to_base64(self):
        """
        :return: DataFrame with spil string
        """

        # open PDF file
        with open(file=self.dir_pdf, mode="rb") as file:
            content = file.read()

            # convert to base64
            base64_content = base64.b64encode(content).decode("utf-8")

            # split string
            str_length = len(base64_content)
            step = 100
            loop_count = math.ceil(str_length / step)

            dict_split = {}

            for i in range(0, loop_count, 1):
                str_split = base64_content[i * step: (i + 1) * step]
                dict_split[i] = str_split

            # save data into pandas
            df_pdf = pd.DataFrame.from_dict(data=dict_split, orient="index", columns=["base64"], dtype="str")

            # reset index
            df_pdf.index.name = "base64_order"
            df_pdf.reset_index(drop=False, inplace=True)

            # create DataFrame
            file_name = Path(self.dir_pdf).name
            df_file = pd.DataFrame([[file_name]], index=[i for i in range(0, loop_count, 1)], columns=["file_name"])

            # concatenate DataFrame
            df_merged = pd.concat([df_file, df_pdf], axis=1)

            return df_merged


class OfficeAutomation:
    """
    Create often used script to handle Office work
    """

    def __init__(self, dir_src, dir_dst):
        """
        Initialization for attributes
        :param dir_doc: path like | absolute directory of raw data
        """
        self.dir_src = dir_src
        self.dir_dst = dir_dst

    def pptx_to_pdf(self):
        """
        Save PPT as PDF
        :return: None
        """

        # create instance of PPT
        powerpoint = comtypes.client.CreateObject('PowerPoint.Application')

        # visible for PPT
        powerpoint.Visible = True

        # Open PPT
        presentation = powerpoint.Presentations.Open(self.dir_src)

        # save as PDF
        presentation.SaveAs(self.dir_dst, 32)

        # close and quit
        presentation.Close()
        powerpoint.Quit()

    def ppt_to_images(self):
        """
        Create pictures for each slide
        :return: None
        """

        # create instance of PPT
        ppt_app = win32.gencache.EnsureDispatch('PowerPoint.Application')

        # visible for PPT
        ppt_app.Visible = True

        # open PPT
        ppt = ppt_app.Presentations.Open(self.dir_src)

        # loop for each slide
        for slide_index in range(1, ppt.Slides.Count + 1):
            # get each slide
            slide = ppt.Slides(slide_index)

            # output for each image
            image_path = os.path.join(self.dir_dst, f"slide_{slide_index}.png")

            # export as image
            slide.Export(image_path, "PNG")

        # close and quit
        ppt.Close()
        ppt_app.Quit()
