import platform
import imp
import smtplib
import os
import cx_Oracle
import datetime
import getpass
import pandas as pd


class DbConn(object):
    """
    Connection to the Oracle Database
    """

    def __init__(self, service):
        """
        Initiate variables
        """
        self.service = service
        self.conn_info = {}
        self.connection = None

    def __get_service(self):
        """
        Assumes service is either mdm or mcdm
        Return full information for database connection
        """
        if self.service == "--mdm":
            self.conn_info = {'user': username,
                              'pw': password,
                              'host': '???',
                              'port': '???',
                              'SID': '???',
                              'service': '???'}
        elif self.service == "--mcdm":
            self.conn_info = {'user': username,
                              'pw': password,
                              'host': '???',
                              'port': '???',
                              'SID': '???',
                              'service': '???'}
        else:
            print(self.service, 'not found.')
        return self.conn_info

    def make_connection(self):
        """
        Login into database with the information gathered from the previous functions
        Return connection of the database
        """
        self.connection = cx_Oracle.connect('{user}/{pw}@{host}:{port}/{service}'.format(**self.__get_service()))
        return self.connection

    def close(self):
        return self.connection.close()

    def __str__(self):
        """
        Show service name
        """
        return self.service


class QueDict(object):
    """
    Write queries into dictionary for quick reference
    """

    def __init__(self):
        """
        Create empty query dictionary
        """
        self.sql_dict = {}
        self.content = None
        self.folder = os.listdir(os.getcwd())

    def formatting(self, sql_content):
        """
        Assumes contents in the .sql files are texts (strings)
        Return service identifier and sql commands in a list
        """
        split_content = sql_content.splitlines()
        db_service = split_content[0]
        qs = ' '.join(split_content[1:]).replace("startt", starting_date).replace("endd", ending_date)
        self.content = [db_service, qs]
        return self.content

    def dictionary(self):
        """
        Write formatted sql contents into dictionary
        """
        for fi in self.folder:
            if fi.endswith(".sql"):
                f = open(fi, 'r')
                sf = f.read()
                self.sql_dict[fi] = self.formatting(sf)
        if bool(self.sql_dict) == False:
            print("No query found in the dictionary.")
            raise ValueError
        return self.sql_dict

    def __str__(self):
        """
        Show the sql dictionary
        """
        return self.sql_dict


# further customization is needed for emailing reports, but currently is working before the email function
def main():
    """
    Execute sql commands and print pandas dataframe
    """
    for file_name, sql_command in QueDict().dictionary().items():
        print(" ")
        print("|+ %s +|" % file_name.upper())
        try:
            c = DbConn(sql_command[0])
            pandas_dataframe = pd.read_sql(sql_command[-1], c.make_connection())
            print(pandas_dataframe)
            c.close()
        except cx_Oracle.OperationalError, msg:
            return "Error occurted while executing sql command, ", msg


"""
email_text = "sss"

class EmailConn(object):
    """
Creating
email
connection
"""
def __init__(self):
    self.ppl = pd.read_csv('contacts.txt', sep=':', names=["name", "email"])
    self.login_info = {}

def get_info(self):
    with open('senders_info.txt', 'r') as f:
        for information in f:
            information = information.split(':')
            self.login_info[information[0]] = information[1].replace('\n', '')
        f.close()
    return self.login_info

def login_n_send(self):
    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(self.get_info()['email'], self.get_info()['password'])
        server.sendmail(self.get_info()['email'], self.ppl['email'].tolist(), email_text)
        server.quit()
    except:
        print("Failed to send the email.")

"""

if __name__ == "__main__":
    current_date = datetime.datetime.today().strftime('%d-%b-%y')
    username = raw_input('Please enter your username: ')
    password = getpass.getpass('Please enter your password: ')
    starting_date = raw_input("Please enter starting date, or hit Enter to use 01-Jan-11: ") or "01-Jan-11"
    ending_date = raw_input("Please enter ending date, or hit Enter to use today's date: ") or current_date

    main()
