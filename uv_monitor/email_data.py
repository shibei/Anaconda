import public_class as public
import info 
import os 
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders

class Mail(object):
    """docstring for Mail"""
    def __init__(self, pathList,day = 1,cycle = 1 ):
        # super(Mail, self).__init__()
        self.pathList = pathList
        self.day = day
        self.cycle = cycle
        self.senderList = info.emailSender(r'/root/ipython/user_list.xlsx', r'email_sender')
        self.receiverList = info.emailReceiver(r'/root/ipython/user_list.xlsx', r'email_receive')
        

    def sendEmail(self):
        pathList = self.pathList
        day = self.day
        cycle = self.cycle 
        senderList = self.senderList
        receiverList = self.receiverList
        user = senderList['qq']['user']
        pwd = senderList['qq']['password']
        to = ','.join(receiverList)
        msg = MIMEMultipart()
        title = '%s周对比分析_%s' % (public.chineseNumber(cycle), str(public.getDate(day)))
        message = '附件为%s的%s周对比分析结果' % (str(public.getDate(day)), public.chineseNumber(cycle))
        msg['Subject'] = title
    #     msg['From'] = r'UV_MONITOR'
        content1 = MIMEText(message, 'plain', 'utf-8')
        msg.attach(content1)
        for path in pathList:
            attfile = path
            basename = os.path.basename(attfile)
            fp = open(attfile, 'rb')
            att = MIMEText(fp.read(), 'base64', 'utf-8')
            att["Content-Type"] = 'application/octet-stream'
            att.add_header('Content-Disposition', 'attachment',
                           filename=('gbk', '', basename))
            encoders.encode_base64(att)
            msg.attach(att)
        #-----------------------------------------------------------
        s = smtplib.SMTP('smtp.qq.com')
        s.login(user, pwd)
        s.sendmail(user, to, msg.as_string())
        s.close()