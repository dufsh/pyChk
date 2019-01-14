#!/usr/bin/python
#coding:utf-8

import sys
import os
import time
import logging
import logging.config
import ConfigParser
import cx_Oracle
import Demo_sms
from apscheduler.schedulers.blocking import BlockingScheduler

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
#os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'


def MsgSender(db_url, defult_recever):

    def getNewMsg():
        '''
            查询新增的预警消息
        '''
        
        v_sql='''select alarm_id,to_char(createtime,'yyyy-mm-dd hh24:mi:ss') as createtime,alarm_type,alarm_level,alarm_text,send_state from zsjk_alarm where createtime>=sysdate-1/24 and send_state in (0,-1) and is_project=0'''

        MsgSet=set()
        try:
            logger.info('getNewMsg exec sql =  %s' , v_sql)
            cur=con.cursor()
            cur.execute(v_sql)
            res=cur.fetchall()

            logger.info('getNewMsg data number is %d' , len(res))
            
            for MsgList in res:
                logger.info('getNewMsg is : %s' , str(MsgList))
                MsgSet.add(MsgList)
                            
        except Exception:
            logger.error('getNewMsg faild  ', exc_info=True)
        finally:
            cur.close()
            return MsgSet

    def getProject(createtime,alarm_type):
        '''
            判断是否为工程状态
        '''
        
        v_sql='''select project_id,createtime,create_user,status,starttime,endtime,project_ne from zsjk_project p where p.status=1 and project_ne=:x1 and p.starttime <=to_date(:x2,'yyyy-mm-dd hh24:mi:ss') and p.endtime>=to_date(:x2,'yyyy-mm-dd hh24:mi:ss') and rownum=1'''
        try:
            logger.info('getProject exec sql =  %s [ %s,%s ]' , v_sql,alarm_type,createtime)
            cur=con.cursor()
            cur.execute(v_sql,x1=alarm_type,x2=createtime)
            res=cur.fetchall()

            #logger.info('getProject data number is %d' , len(res))

            if len(res)>=1:
                 logger.info('getProject is : %s' , str(MsgList))
                 return 1
            else:
                logger.info('not get Project')
                return 0
        except Exception:
            logger.error('getNewMsg faild  ', exc_info=True)
            return -1
        finally:
            cur.close()
            
    def updateMsgProject(alarm_id):
        '''
            更新告警为工程状态，0非工程、1工程
        '''
        
        v_sql='''update zsjk_alarm set is_project=1 where alarm_id=:x1'''

        try:
            logger.info('updateMsgProject exec sql =  %s [ %d ]' , v_sql,alarm_id)
            cur=con.cursor()
            cur.execute(v_sql,x1=alarm_id)
            con.commit()
        except Exception:
            logger.error('updateMsgProject faild  ', exc_info=True)
        finally:
            cur.close()
            
    def getUsers(alarm_type):
        '''
            获取需要发送短信的人员信息
        '''
        v_sql="select user_id,user_name,phone_number,mobile_from,user_info,role_name,alarm_type from zsjk_user_v where ','||alarm_type||',' like '%,'||:x1||',%'"

        userSet=set()
        try:
            logger.info('getUsers exec sql =  %s [ %s ]' , v_sql,alarm_type)
            cur=con.cursor()
            cur.execute(v_sql,x1=alarm_type)
            res=cur.fetchall()

            logger.info('getUsers data number is %d' , len(res))
            for UserList in res:
                #v_user_name=UserList[1]
                v_phone_number=UserList[2]
                v_mobile_from=UserList[3]
                #logger.info('getUsers is : %s' , str(UserList))
                v_cmcc_mobile='移动号段'
                if v_mobile_from!=v_cmcc_mobile:
                    logger.info('this phone_number %d is %s , not need send .' , v_phone_number,v_mobile_from)
                else:
                    logger.info('this phone_number %d is %s , need send .' , v_phone_number,v_mobile_from)
                    userSet.add(v_phone_number)
        
        except Exception:
            logger.error('getNewMsg faild  ', exc_info=True)
        finally:
            cur.close()
            return userSet


    def updateMsgSendState(alarm_id,send_state):
        '''
            #更新发送状态：0未发送、1已发送、-1发送失败、-2未配置发送对象
        '''
        
        v_sql='''update zsjk_alarm set send_state=:x1,send_time=sysdate where alarm_id=:x2'''

        try:
            logger.info('updateMsgSendState exec sql =  %s [ %d,%d ]' , v_sql,send_state,alarm_id)
            cur=con.cursor()
            cur.execute(v_sql,x1=send_state,x2=alarm_id)
            con.commit()
        except Exception:
            logger.error('updateMsgSendState faild  ', exc_info=True)
        finally:
            cur.close()                           

    def getDefultRecever():
        '''
        '''

    startTime=time.time()
    logger.info('task start ... ')

    MsgSet=set()
    v_userSet=set()
    v_is_project=0
    usersStr=''

    try:
        con=cx_Oracle.connect(db_url)
        MsgSet=getNewMsg()
        if len(MsgSet)>0:
            for MsgList in MsgSet:
                v_alarm_id=MsgList[0]
                v_createtime=MsgList[1]
                v_alarm_type=MsgList[2]
                v_alarm_text=MsgList[4].decode('utf-8')
            
                v_is_project=getProject(v_createtime,v_alarm_type)
                if v_is_project==1:
                    logger.info('it is project, not need send msg.')
                    updateMsgProject(v_alarm_id)
                elif v_is_project==0:
                    logger.info('it is not project.')
                    v_userSet=getUsers(v_alarm_type)
                    if len(v_userSet)>0:
                        i=0
                        for user in v_userSet:
                            if i==0:
                                usersStr=str(user)
                            else:
                                usersStr=usersStr+','+str(user)
                            i=i+1
                        logger.info('get user to need send msg is : %s.',usersStr)

                        #print v_alarm_text
                        res=Demo_sms.sendSms(usersStr,v_alarm_text)
                        if res:
                            logger.debug('send sms to %s sucess',usersStr)
                            updateMsgSendState(v_alarm_id,1)
                        else:
                            logger.debug('send sms failed.')
                            updateMsgSendState(v_alarm_id,-1)
                    else:
                         logger.info('not get user to need send msg.')
                         usersStr='13730885681'
                         v_alarm_text='no recever '+ MsgList[4].decode('utf-8')
                         res=Demo_sms.sendSms(usersStr, v_alarm_text)
                         logger.debug('send sms to %s',usersStr)
                         updateMsgSendState(v_alarm_id,-2)

    except Exception:
        logger.error('connect db faild : ' , exc_info=True)
        return
    finally:
        con.close()
    
    endTime=time.time()
    timeCost= round(endTime - startTime ,2)
    logger.info('task finished , used time %d s' , timeCost )


if ( __name__ == "__main__"):

    logging.config.fileConfig("logging.conf")
    #create logger
    logger = logging.getLogger("MsgSender")
    
    cfgfile='conf/MsgSender.conf'
    try:
        cf = ConfigParser.SafeConfigParser()
        cf.read(cfgfile)
        db_url = cf.get('db_info', 'db_url')
        defult_recever = cf.get('sms', 'defult_recever')
    except Exception:
        logger.error('parse cfg file %s failed : ' ,cfgfile, exc_info=True)
        sys.exit(-1)
    
    try:
        scheduler = BlockingScheduler()
        scheduler.add_job(MsgSender, 'cron', args=(db_url, defult_recever), second='0/30')
        while True:
            scheduler.start()    #采用的是阻塞的方式，只有一个线程专职做调度的任务
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

