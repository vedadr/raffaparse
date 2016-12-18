import httplib2
import os
import base64 #used to decode attachment
from bs4 import BeautifulSoup #for attachment parsing
import pandas as pd #for storage prior to sqlite3
from dateutil.parser import parse as dtpa #used to check if first cell contained date info
import sqlite3

from apiclient import discovery, errors
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'gmail-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def getRaffaMails(service, user_id, query):
    try:
        response = service.users().messages().list(userId=user_id, q=query).execute()
        messages = []
        if 'messages' in response:
          messages.extend(response['messages'])
        while 'nextPageToken' in response:
          page_token = response['nextPageToken']
          response = service.users().messages().list(userId=user_id, q=query,
                                                     pageToken=page_token).execute()
          messages.extend(response['messages'])
        return (messages)
    except errors.HttpError as error:
        print('An error occurred: {}'.format(error))


def isDateTime(testString):
    try:
        dtpa(testString)
        return True
    except:
        return False


def scrapeStatementForInfo(attachment):
    data = []
    soup = BeautifulSoup(attachment)
    rows = soup.find_all('tr')
    for td in rows:
        rowData = td.find_all('td')
        if (rowData):
            rowText = rowData.find(text = True)
            if (isDateTime(rowText)):
                data.append(rowData)
    return data

def downloadMails(raffaMailIds, service, user_id):
    messagesHolder = []
    attachHolder = []
    tempCount = 0
    if tempCount < 2:
        for el in raffaMailIds:
            downloadedMsg = service.users().messages().get(userId=user_id, id=el['id']).execute()
            for part in downloadedMsg['payload']['parts']:
                if part['filename'] and part['mimeType'] == 'text/html':
                    attachmentId = part['body']['attachmentId']
                    attachmentData = service.users().messages().attachments().get(userId = user_id, messageId = el, id = attachmentId).execute()
                    attachHolder.append(scrapeStatementForInfo(base64.urlsafe_b64decode(attachmentData['data'].encode('UTF-8'))))
            tempCount = tempCount + 1
        return (messagesHolder)

def main():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)

    raffaMailIds = getRaffaMails(service, 'me', 'from:E-izvodi.Raiffeisenbanka@rbbh.ba')
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS messageIds')
    cur.execute('''CREATE TABLE IF NOT EXISTS messageIds(
                    threadId TEXT,
                    msgID TEXT
                    )''')
    cur.executemany('INSERT INTO messageIds (threadId, msgId) VALUES (:threadId, :id);', raffaMailIds)
    messageList = downloadMails(raffaMailIds, service, 'me')

    print('stop here')

if __name__ == '__main__':
    reloadAllMessages = False

    try:
        import argparse

        flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
    except ImportError:
        flags = None

    # If modifying these scopes, delete your previously saved credentials
    # at ~/.credentials/gmail-python-quickstart.json
    SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
    CLIENT_SECRET_FILE = 'client_secret.json'
    APPLICATION_NAME = 'Gmail API Python Quickstart'

    conn = sqlite3.connect('database.sqlite')
    main()
