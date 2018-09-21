# -*- coding: utf-8 -*-
from __future__ import print_function   
#coding=utf-8
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import pandas as pd
import json
import numpy as np
import subprocess

import datetime
import oauth2client
import sys
# import MySQLdb
import codecs
import re

'''
with open('yaoyao_insert.json', 'r') as insert:
      text = insert.read()
for line in test:
  paras = json.loads(line)
  SQL=paras['sql']
  spreadsheetId = paras['spreadsheetId']
  rangeName = paras['rangeName']
'''
try:
    import argparse

    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()  # ArgumentParser建立解析器对象
    # parse_args() 的返回值是一个命名空间，包含传递给命令的参数。该对象将参数保存其属性，因此如果你的参数 dest 是 "myoption"，
    # 那么你就可以args.myoption 来访问该值。
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
# SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
# https://www.googleapis.com/auth/spreadsheets
SCOPES = 'https://www.googleapis.com/auth/spreadsheets'  # 表格网址
CLIENT_SECRET_FILE = 'client_secret.json'  #
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


# spreadsheets.readonly

def get_credentials():
    home_dir = os.path.expanduser('~')  # 把path中包含的"~"和"~user"转换成用户目录
    credential_dir = os.path.join(home_dir, '.credentials')  # 连接
    if not os.path.exists(credential_dir):  # 没有这个目录则创建
        os.makedirs(credential_dir)  # 递归创建目录
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')  # 连接
    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def out_put(service, spreadsheetId, rangeName):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    values = result.get('values', [])
    if not values:
        print('No data found.')
    else:
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            print('%s, %s' % (row[0], row[4]))


def addDate(service, values, spreadsheetId, rangeName):
    body = {
        'values': values
    }
    result = service.spreadsheets().values().append(
        spreadsheetId=spreadsheetId, range=rangeName, body=body, valueInputOption='USER_ENTERED').execute()
    print(result)


def updateData(service, spreadsheetId, rangeName, values):
    print(rangeName)
    print(str(values))
    body = {
        'values': values
    }
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheetId, range=rangeName, body=body, valueInputOption='RAW').execute()
    print(result)


def getvalues(sel_amazon):
    
    LIB_BASE_DIR = "/home/gujinkai"
    athena_shell = "java -jar " + LIB_BASE_DIR + "/athena_sql_accesskey.jar "
    p = subprocess.Popen(athena_shell + "\"" + sel_amazon + "\""+" AKIAIMKAC2JZTWZM2QSQ", shell=True, stdout=subprocess.PIPE)
    result = p.communicate()
    result = list(result)
    result = result[0].decode()
    if len(result) != 0:
        result = result.split("\n")
        result.remove('')
        for i in range(len(result)):
            result[i] = result[i].split("\t", 50)
        result = pd.DataFrame(result)

    values = np.array(result)  # np.ndarray()
    values = values.tolist() # list
    return values
'''
    f = open("/home/gujinkai/date/output.txt")
    lines = f.readlines()
    wd = ''
    wd_last = ''
    row = []
    tx = []
    for line in lines:
        for word in line:
            if '\n' in word:
                tx.append(row)
                row = []
            elif '\t' in word:
                try:
                    wd = float(wd)
                except:
                    wd = wd
                else:
                    wd = float(wd)
                if wd == '':
                    if wd_last == 'e2934742f9d3b8ef2b59806a041ab389':
                        wd = 'ikey'
                    elif wd_last == '78472ddd7528bcacc15725a16aeec190':
                        wd = 'kika'
                    elif wd_last == '4e5ab3a6d2140457e0423a28a094b1fd':
                        wd = 'pro'
                row.append(wd)
                wd_last = wd
                wd = ''
            else:
                wd = wd + word
    values = tx
'''



# re.sub(pattern, repl, string, count=0, flags=0)


def main(spreadsheetId, rangeName,SQL):
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)
    # out_put(service,spreadsheetId,rangeName)
    print(SQL)
    values = getvalues(SQL)
    print(values)
    addDate(service, values, spreadsheetId, rangeName)
   # updateData(service,spreadsheetId,rangeName,values)


if __name__ == '__main__':
    
   with open('yaoyao_insert.json', 'r') as insert:
        # line=insert.read() 
     for line in insert:
         print(line)
         paras = json.loads(line)
         SQL=paras['sql']
         spreadsheetId = paras['spreadsheetId']
         rangeName = paras['rangeName']
         main(spreadsheetId, rangeName,SQL)

