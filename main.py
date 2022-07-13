import email
import imaplib
import re
import os
from datetime import datetime

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
    email_message = email.message_from_string(raw_email_string)
    # Getting mail's body
    for payload in email_message.get_payload():
        body = payload.get_payload(decode=True).decode('utf-8')
        # Getting targeted statistic
        dates = re.findall(r'\d{2}/\d{2}/\d{4}\r\n\t\t\d*', body)
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


readmail(os.getenv('SUBJ'))
