#!/usr/bin/python
#coding:utf-8
''' usage : sys.argv[0] 'telnums' 'message' '''

import sys
import ConfigParser
from suds.client import Client
import logging
import logging.config

try:
    cf = ConfigParser.SafeConfigParser()
    cf.read('conf/Sms.conf')
    url = cf.get('Sms', 'url')
    operation = cf.get('Sms', 'operation')
    username = cf.get('Sms', 'username')
    password = cf.get('Sms', 'password')
    licence = cf.get('Sms', 'licence')
    msgTemp = '''<?xml version='1.0' encoding='GBK'?>
<xml>
  <message>
    <OneRecord>
      <desttermid>__telNum__</desttermid>
      <username>__username__</username>
      <password><![CDATA[__password__]]></password>
      <licence><![CDATA[__licence__]]></licence>
      <msgcontent><![CDATA[__message__]]></msgcontent>
      <systeminfo><![CDATA[ISS]]></systeminfo>
    </OneRecord>
  </message>
</xml>'''

except Exception as e:
    print ' get error : ' + str(e)


logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

def sendSms(telNums,message):
    flg= True
    try:
        c = Client(url)
        Msg=msgTemp.replace('__username__',username).replace('__password__',password).replace('__licence__',licence).replace('__message__',message)
        telLists=telNums.split(',')
        for telNum in telLists:
            sendMsg=Msg.replace('__telNum__',telNum)
            logger.debug(telNum)
            logger.debug(sendMsg)
            results = c.service.SmsSendMQ(sendMsg)

            for result in results:
                if result=='0':
                    logger.info('send message ok.')
                else:
                    flg= False
                    logger.error('send message failed !')
    except Exception as e:
        logger.error(' get error : ' ,exc_info=True )
        flg= False
    return flg

if ( __name__ == "__main__"):
    if len(sys.argv) <3:
        print 'argument is ' + str(len(sys.argv)) + ' , less 3 , exp : ' + sys.argv[0] + ' telnums message '
        sys.exit(-1) 

    telNums=sys.argv[1]
    try:
        message=sys.argv[2].decode('gbk')
    except Exception as e:
        message=sys.argv[2].decode('utf-8')

    sendSms(telNums,message)
