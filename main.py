import email
import imaplib
import re
import os
# from datetime import datetime
import sqlalchemy
from sqlalchemy.types import DATE
from sqlalchemy.types import INT
from sqlalchemy import engine as sql
import pymysql
import pandas as pd

# Connect to the mail server
# subjs = ['Subject Mogilev', 'Subject Smolevitchi']
mail = imaplib.IMAP4_SSL('mail.servolux.by')
mail.login(os.getenv('SENT_MAIL'), os.getenv('SENT_PASS'))
mail.list()
mail.select("inbox")


# Work with a mail
def readmail(subj):
    # Search a targeted mail by subject
    result, data = mail.search(None, subj)
    ids = data[0]
    id_list = ids.split()
    # Getting the last mail
    latest_email_id = id_list[-1]
    # Pulling and decoding
    result, data = mail.fetch(latest_email_id, "(RFC822)")
    raw_email = data[0][1]
    raw_email_string = raw_email.decode('utf-8')
    b = email.message_from_string(raw_email_string)
    if b.is_multipart():
        for part in b.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))

            # skip any text/plain (txt) attachments
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body = part.get_payload(decode=True).decode('utf-8')  # decode
                break
    # not multipart - i.e. plain text, no attachments
    else:
        body = b.get_payload(decode=True).decode('utf-8')

    dates = re.findall(r'\d{2}/\d{2}/\d{4}', body)
    values = re.findall(r'\d+', str(re.findall(r'>\d+<', body)))

    for i in range(len(dates)):
        mid_date = dates[i].split('/')
        mid_date.reverse()
        s = ''.join(mid_date)
        dates[i] = s

    # Make connection to database
    eng = sql.create_engine("mysql+pymysql://sentinel:123.Qaz@10.20.7.177:3306/ishida")
    conn = eng.connect()

    # Create pandas dataframe
    df = pd.DataFrame({'date': dates, 'value': values})
    print(df)
    # Create temporary table and upload DataFrame
    conn.execute("""CREATE TEMPORARY TABLE temp_table (date DATE primary key, value INT)"""
    )
    # Insert data to the table
    # re.findall(r'\s+(\S+)$', os.getenv('SUBJ'))[0]
    df.to_sql('temp_table', conn, if_exists='append', index=False, dtype={'date': DATE, 'value': INT})

    # Merge temp_table into main table
    conn.execute("""
                INSERT INTO {main_table} (date, value) 
                SELECT date, value FROM temp_table
                ON DUPLICATE KEY UPDATE value = temp_table.value
    """.format(main_table=re.findall(r'\s+(\S+)$', os.getenv('SUBJ'))[0]))

    conn.close()
    '''
    to_zabb = []
    # Creating a list of values
    if dates:

        for raw in dates:
            # Splitting to key-value pairs
            to_zabb.append(raw.split('\r\n\t\t'))
        # Sorting (cause sometimes statistic comes with wrong order) and taking the newest info
        ordered = \
            sorted(to_zabb, key=lambda x: datetime.strptime(x[0], "%d/%m/%Y").strftime("%Y-%m-%d"), reverse=True)[0]
        # A little logging
        print(ordered)
        # Sending to zabbix os.system('zabbix_sender -z zabbix_host -s "host_name" -k {item_key} -o {
        # value}'.format(item_key=ordered[0], value=ordered[1]))
        os.system('zabbix_sender -z {zhost} -s "{zname}" -k {zkey} -o {value}'.format(zhost=os.getenv('ZHOST'),
                                                                                      zname=os.getenv('ZNAME'),
                                                                                      zkey=os.getenv('ZKEY'),
                                                                                      value=ordered[1]))
    #print(email_message)
    # Getting mail's body

    '''


readmail(os.getenv('SUBJ'))
