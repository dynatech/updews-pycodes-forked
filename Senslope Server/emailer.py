import smtplib
import email.utils
from email.mime.text import MIMEText

import imaplib
from email.parser import HeaderParser
import datetime


def sendmessage(user,pw,toaddr,fromaddr,subject,msg):
    # Specifying the from and to addresses
    # message
    msg = MIMEText(msg)
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = toaddr
    
    # Sending the mail  
    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.starttls()
        server.login(user,pw)
        server.sendmail(fromaddr, [toaddr], msg.as_string())
        server.quit()
        print 'email sent to ', toaddr
    except smtplib.SMTPException as errmsg:
        print 'Caught exception smtplib.SMTPException: %s.\n>>>>>Unable to send email this time'% errmsg
    except smtplib.socket.gaierror as errmsg:
        print 'Caught exception smtplib.socket.gaierror: %s.\n>>>>>Unable to send email this time'% errmsg
    except smtplib.socket.error as errmsg:
        print 'Caught exception smtplib.socket.error: %s\n>>>>>Unable to send email this time'% errmsg
       
    
def getmessagefromsender(addr,pw,sender):
    
    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    mail.login(addr, pw)
    mail.list()
    # Out: list of "folders" aka labels in gmail.
    mail.select("inbox") # connect to inbox.
    #typ, data = mail.search(None, '(FROM "")')
    searchstring = '(FROM "'+sender+ '")'
    
    typ, num = mail.search(None, searchstring)
    msg=mail.fetch(num[0][-1], '(RFC822)')
    
    header_data = msg[1][0][1]
    
    parser = HeaderParser()
    msg = parser.parsestr(header_data)
    
#    date=msg['Date']
    date=msg['Date']
    date = email.utils.parsedate_tz(date)
    date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date))
    
    subj=msg['Subject']
#    fromaddr = email.utils.parseaddr(msg['From'])[1]
    text= msg.get_payload(decode=True)

    mail.logout()
    return subj,cleantext(text),date

def cleantext(txt):
        txt=txt.replace(u'\r\n','\n')
        txt=txt.replace(u'\r','')
        txt=txt.replace(u'\n','')
        txt=txt.replace(u'\t',' ')
        txt=txt.encode('utf8')
        
        return txt
