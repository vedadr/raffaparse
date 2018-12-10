import httplib2
import os
import base64  # used to decode attachment
from bs4 import BeautifulSoup  # for attachment parsing
import pandas as pd  # for storage prior to sqlite3
from dateutil.parser import parse as dtpa  # used to check if first cell contained date info
import sqlite3
import pickle

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
    credential_path = os.path.join(credential_dir, 'gmail-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        # else:  # Needed only for compatibility with Python 2.6
        #     credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_raffa_mails(service, user_id, query):
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
        return messages
    except errors.HttpError as error:
        print('An error occurred: {}'.format(error))


def is_date_time(testString):
    try:
        dtpa(testString)
        return True
    except:
        return False


def scrapeStatementForInfo(attachment):
    data = []
    soup = BeautifulSoup(attachment, "lxml")
    tables = soup.find_all('table')[7:]
    data_content = []
    for i, table in enumerate(tables):
        cells = table.find_all('td')
        cells_content = [c.text for c in cells]

        if i == 0:
            columns = cells_content
        else:
            if is_date_time(cells_content[0]):
                data_content.append(cells_content)
            else:
                break
    email_content = pd.DataFrame(data=data_content, columns=columns)

    return email_content


def fetch_mails(raffa_mail_ids, service, user_id):
    """
    This will download emails with selected ids
    :param raffa_mail_ids: list of mail id's
    :param service:
    :param user_id:
    :return: list of messages
    """
    temp_count = 0
    if temp_count < 2:
        if pickle_mail_content:
            attach_holder_raw = []
            for el in raffa_mail_ids:
                downloaded_msg = service.users().messages().get(userId=user_id, id=el['id']).execute()
                for part in downloaded_msg['payload']['parts']:
                    if part['filename'] and part['mimeType'] == 'text/html':
                        attachment_id = part['body']['attachmentId']
                        attachment_data = service.users().messages().attachments().get(userId=user_id, messageId=el,
                                                                                       id=attachment_id).execute()
                        attach_holder_raw.append(base64.urlsafe_b64decode(attachment_data['data'].encode('UTF-8')))
                temp_count = temp_count + 1
            pickle.dump(attach_holder_raw, open('attachments_raw.pkl', 'wb'))
        else:
            attach_holder_raw = pickle.load(open('attachments_raw.pkl', 'rb'))

    return attach_holder_raw


def process_mails(mail_att_extra):
    attachment_holder = []
    for attachment_raw in mail_att_extra:
        attachment_holder.append(scrapeStatementForInfo(attachment_raw))
    processed_emails = pd.concat(attachment_holder)
    processed_emails.to_sql('usage_data', con=conn)


def main():
    # prepare authentication
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)

    if update_mails:
        # get mail ids
        raffa_mail_ids = get_raffa_mails(service, 'me', 'from:E-izvodi.Raiffeisenbanka@rbbh.ba')

        # prepare db and store mail id's
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS messageIds')
        cur.execute('''CREATE TABLE IF NOT EXISTS messageIds(
                        threadId TEXT,
                        msgID TEXT
                        )''')
        cur.executemany('INSERT INTO messageIds (threadId, msgId) VALUES (:threadId, :id);', raffa_mail_ids)
    else:
        cur = conn.cursor()
        cur.execute('SELECT * FROM messageIds')
        raffa_mail_ids = cur.fetchall()
    # download mails
    mail_attachments_extracted = fetch_mails(raffa_mail_ids, service, 'me')
    process_mails(mail_attachments_extracted)


if __name__ == '__main__':
    # reloadAllMessages = False
    # if set to true script will check if there are new mails available
    update_mails = False
    # if set to false script will use pickled email attachments (won't download new ones)
    pickle_mail_content = False

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
