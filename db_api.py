#!/usr/bin/python
#encoding:utf8


from bsddb3 import db
import os
import re
import sys
import time
import Queue
import socket
import hashlib
import MySQLdb
import datetime
import threading


###########全局变量##################
SQL_MAX_LEN = 1024*1024*1024 #sql语句的最大长度
CACHE_LIST = []#抓取数据缓存列表
CACHE_LIST_LEN = 0#缓存列表的长度
CACHE_LEN_MAX = 20000#缓存列表允许的最大长度
breakNum = 0
lock = threading.Lock()
CRAWL_DB = db.DB()
CRAWL_DB.open("/laiseek/weibo_db/db_file/except_file.crawl",dbtype=db.DB_HASH,flags=db.DB_CREATE)#存储出现异常的字典数据的文件
SQL_DB = db.DB()
#存储出现异常的sql数据的文件
SQL_DB.open("/laiseek/weibo_db/db_file/except_file.sql",dbtype=db.DB_HASH,flags=db.DB_CREATE)
META_IP = "202.121.97.61"#metadata通信的ip
META_PORT = 19886#metadata通信的端口
table_schema = {'weibo_user':{},'weibo_data':{},'weibo_follow':{}}#存储表结构
LOG_FILE = open('log','w')#日志文件
mutex = threading.Lock()
####################################

class ThreadInsert(threading.Thread):
  def __init__(self,sql_tuple):
    threading.Thread.__init__(self)
    self.sql_tuple = sql_tuple
    #LOG_FILE.write("ThreadInsert start\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')
    print 'count',threading.activeCount()

  def run(self):
    global SQL_DB
    global mutex

    table_name,sql_seq,values = self.sql_tuple
    #LOG_FILE.write('insert sql\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + sql_seq + '\n')
    while True:
      try:
        server_ip,sig = self.insert_req(table_name)
        print server_ip,table_name,sig
      except Exception,e:
        mutex.acquire()
        SQL_DB.put(md5_hash(self.sql_tuple),str(self.sql_tuple))
        mutex.release()
        break
      else:
        if sig == 'yes':#允许插入
          try:
            #print 'wrong**************************************wrong'
            mysql_db = MySQLdb.connect(host=server_ip,user="root",passwd="i@ls!",db="weibo",charset='utf8')
            cur = mysql_db.cursor()
            cur.execute(sql_seq)
            self.insert_result("""{'type':'insert_result','data':{'result':'ok','table':'%s','key':'%s'}}"""%(table_name,values),self.getName())
            #LOG_FILE.write('sucess insert sql\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + sql_seq + '\n')
          except Exception,e:
            #print 'wrong**************************************wrong'
            print e,server_ip,table_name,sql_seq
            mutex.acquire()
            SQL_DB.put(md5_hash(self.sql_tuple),str(self.sql_tuple))
            mutex.release()
            self.insert_result("""{'type':'insert_result','data':{'result':'%s','table':'%s','sql':'%s'}}"""%(str(e),table_name,sql_seq),self.getName())
        if sig == 'delay':#延迟
          time.sleep(3)#延迟3s后再执行插入
          continue
        if sig == 'no':#不能插入
          mutex.acquire()
          SQL_DB.put(md5_hash(self.sql_tuple),str(self.sql_tuple))
          mutex.release()
          #LOG_FILE.write('sql into bsddb\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + sql_seq + '\n')
        break
    #LOG_FILE.write("ThreadInsert end\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')
    
  def insert_result(self,obj,threadname):
    while True:
      try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((META_IP,META_PORT))
        sock.send(obj)
        sock.close()
        break
      except Exception,e:
        print threadname,'insert_result error',e,obj
        time.sleep(60)

  def insert_req(self,table_name):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((META_IP,META_PORT))
    sock.send("""{'type':'insert_req','param':'%s'}"""%table_name)
    data = sock.recv(1024)
    sock.close()
    data = eval(data)
    return (data['server'],data['state'])
   
class ThreadCrawlInsert(threading.Thread):
  def __init__(self,cache_list):
    threading.Thread.__init__(self)
    self.cache_list = cache_list
    #LOG_FILE.write("ThreadCrawlInsert start\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')

  def run(self):
    global CRAWL_DB
    global breakNum
    #分离数据,再组合
    user_group_result = {}
    content_group_result = {}
    follow_group_result = {}
    while self.cache_list:
      dict_data = self.cache_list.pop()
      try:

        #微博用户数据
        if "uid" in dict_data:
          table_name = self.get_table("""{'type':'get_table','table':'weibo_user','key':'%s'}"""%dict_data['uid'])
          if table_name not in user_group_result:
            user_group_result[table_name] = []
          user_group_result[table_name].append(dict_data)
          #LOG_FILE.write("dict into group\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + str(dict_data) + '\n')
        #微博博文数据
        elif "user_id" in dict_data:
          table_name = self.get_table("""{'type':'get_table','table':'weibo_data','key':'%s,%s'}"""%(dict_data['user_id'],dict_data['created_at'].split(' ')[0]))
          if table_name not in content_group_result:
            content_group_result[table_name] = []
          content_group_result[table_name].append(dict_data)
          #LOG_FILE.write("dict into group\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + str(dict_data) + '\n')
        #微薄关注数据
	elif "fid" in dict_data:
          table_name = self.get_table("""{'type':'get_table','table':'weibo_follow','key':'%s'}"""%dict_data['fid'])
          if table_name not in follow_group_result:
            follow_group_result[table_name] = []
          follow_group_result[table_name].append(dict_data)
          #LOG_FILE.write("dict into group\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + str(dict_data) + '\n')   
        else:
          pass
      except Exception,e:
	print e
        if 'timed out' in str(e):
         print 'timed out:',time.ctime()
         breakNum = 1
	  exit()
        mutex.acquire()
        CRAWL_DB.put(md5_hash(dict_data),str(dict_data))
        mutex.release()
        #LOG_FILE.write("dict into bsddb\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + str(dict_data) + '\n')
    sql_queue = Queue.Queue()
    #组合weibo_user表sql
    self.comb_sql(sql_queue,user_group_result,'weibo_user','uid')
    #组合weibo_data表sql
    self.comb_sql(sql_queue,content_group_result,'weibo_data','mid')
    #组合weibo_follow表sql
    self.comb_sql(sql_queue,follow_group_result,'weibo_follow','fid')
    #插入sql
    while not sql_queue.empty():
      sql_tuple = sql_queue.get()
      t = ThreadInsert(sql_tuple)
      t.start()
      t.join()
    #LOG_FILE.write("ThreadCrawlInsert end\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')

  def comb_sql(self,sql_queue,group_result,table,uni_key):
    '''
    func:将数据组合成sql语句
    param:
         sql_queue,存储sql语句的队列
         group_result,存储数据的列表
         table,数据应该插入的表类型，有用户表和博文表两种
         uni_key,唯一代表数据的键
    '''
    #确定插入的表的结构
    sql_colm = ''
    for colm in table_schema[table].keys():
      sql_colm += colm + ',' 
    sql_colm = sql_colm[:-1]
    for table_name in group_result:
      #组合sql,并记录组成该sql的数据的唯一健的值
      sql = "insert ignore into `%s`(%s) values"%(table_name,sql_colm)
      values = []
      for dict_data in group_result[table_name]:
        data_sql = '('
        for key in table_schema[table].keys():
          if key == 'post_time':
            data_sql += 'NOW(),'
            continue
          if key not in dict_data or dict_data[key] == '':
            data_sql += "''" + ','
          else:
            try:
              data_sql += "'%s'"%MySQLdb.escape_string(str(dict_data[key])) + ','
            except Exception,e:
              data_sql += "'%s'"%MySQLdb.escape_string(str(dict_data[key].encode('utf8'))) + ','
        data_sql = data_sql[:-1] + '),'
        if len(data_sql) + len(sql) >= SQL_MAX_LEN:
          sql_queue.put((table_name,sql[:-1],values))
          #LOG_FILE.write('sql into queue\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + sql + '\n')
          sql = "insert ignore into `%s`(%s) values"%(table_name,sql_colm)
          values = []
        sql += data_sql
        #LOG_FILE.write('dict into sql\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + str(dict_data) + '\n')
        values.append(dict_data[uni_key])
      sql_queue.put((table_name,sql[:-1],values))
      #LOG_FILE.write('sql into queue\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + sql + '\n')

  def get_table(self,send_data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((META_IP,META_PORT))
    sock.send(send_data)
    table_name = sock.recv(1024)
    return eval(table_name)['table']

class ThreadSqlInsert(threading.Thread):
  def __init__(self,sql_list):
    threading.Thread.__init__(self)
    self.sql_list = sql_list
    #LOG_FILE.write("ThreadSqlInsert start\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')
  def run(self):
    for sql_tuple in self.sql_list:
      t = ThreadInsert(sql_tuple)
      t.start()
      t.join()
    #LOG_FILE.write("ThreadSqlInsert end\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')
      
class ThreadMain(threading.Thread):
  def __init__(self,cache_list):
    threading.Thread.__init__(self)
    self.cache_list = cache_list
    #LOG_FILE.write("ThreadMain start\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')

  def run(self):
    #如果有上轮留下的抓取原始数据,将这些数据加入到本轮抓取原始数据缓存列表,然后进行插入,数据存放在文件数据库bsddb中
    '''
    cur = CRAWL_DB.cursor()
    while cur.first():
      key,value = cur.first()
      self.cache_list.append(eval(value))
      #LOG_FILE.write('dict from bsddb\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + value + '\n')
      CRAWL_DB.delete(key)
    #如果有上轮留下的sql语句数据,对这些数据进行插入
    cur = SQL_DB.cursor()
    sql_list = []
    while cur.first():
      key,value = cur.first()
      sql_list.append(eval(value))
      #LOG_FILE.write('sql from bsddb\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + value + '\n')
      SQL_DB.delete(key)
    '''
    threads = []
    #threads.append(ThreadSqlInsert(sql_list))
    threads.append(ThreadCrawlInsert(self.cache_list))
    for i in threads:
      i.start()
    for i in threads:
      i.join()
    #LOG_FILE.write("ThreadMain end\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')

class ThreadFinal(threading.Thread):
  '''
  循环对最后一轮的异常数据进行插入直到所有数据都正确插入
  '''
  def __init__(self,cache_list):
    threading.Thread.__init__(self)
    self.cache_list = cache_list
    #LOG_FILE.write("ThreadFinal start\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')

  def run(self):
    #开启抓取插入线程插入最后的可能未达到阈值的数据列表
    t = ThreadCrawlInsert(self.cache_list)
    t.start()
    t.join()
    while True:
      cache_list = []
      sql_list = []
      '''
      cur = CRAWL_DB.cursor()
      while cur.first():
        key,value = cur.first()
        print key,value
        try:
          cache_list.append(eval(value))
        except Exception,e:
          print e
        print len(cache_list)
        #LOG_FILE.write('dict from bsddb\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + value + '\n')
        CRAWL_DB.delete(key)
      cur = SQL_DB.cursor()
      while cur.first():
        key,value = cur.first()
        sql_list.append(eval(value))
        #LOG_FILE.write('sql from bsddb\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + value + '\n')
        SQL_DB.delete(key)
      '''
      threads = []
      if cache_list:
        threads.append(ThreadCrawlInsert(cache_list))
      if sql_list:
        threads.append(ThreadSqlInsert(sql_list))
      if not (cache_list or sql_list):
        break
      for i in threads:
        i.start()
      for i in threads:
        i.join()
    #LOG_FILE.write("ThreadFinal end\t" + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + self.getName() + '\n')

def md5_hash(obj):
  obj = str(obj)
  number = 0
  while True:
    number += 1
    if number == 10:
      return hashlib.new("md5",obj).hexdigest()
    try:
      obj_md5 = hashlib.new("md5",obj).hexdigest()
      return obj_md5
    except:
      try:
        m = hashlib.md5()
        m.update(obj)
        return m.hexdigest()
      except Exception,e:
        print 'hashlib error',e

def final():
  '''
  循环对最后一轮的异常数据进行插入直到所有数据都正确插入
  '''
  t = ThreadFinal(CACHE_LIST)
  t.start()

def put_weibo_data(dict_data):
  '''
  func:抓取程序调用该函数传送要插入的数据
  para:
      dict_data:抓取数据的字典
  '''

  global CACHE_LIST
  global CACHE_LIST_LEN
  global lock
  global breakNum
  if breakNum == 1:
   return 'stop'
  debug = test_data(dict_data)
  if debug == 'yes':
    #加锁
    lock.acquire()
    CACHE_LIST.append(dict_data)
    CACHE_LIST_LEN += 1
    #列表长度达到阈值
    #print 'CACHE_LIST_LEN:',CACHE_LIST_LEN
    if CACHE_LIST_LEN == CACHE_LEN_MAX:
      #开启插入线程入口,不等待线程结束
      t = ThreadMain(CACHE_LIST)
      t.start()
      CACHE_LIST = []
      CACHE_LIST_LEN = 0
      print '*******************start insert*********************'
      lock.release()
      return 'start insert'
    lock.release()
    #LOG_FILE.write('into cache_list\t' + time.strftime("%Y-%m-%d %H:%M:%S") + '\t' + str(dict_data) + '\n')
  elif debug == 'no':
    return False
  else:
    pass
  return True

def test_data(dict_data):
  '''
  func:测试抓取数据的准确性
  param:
       抓取到的原始数据
  '''
  #数据是微博用户数据
  if 'uid' in dict_data and dict_data['uid']:
    return 'yes'
  #数据是微博博文数据
  if 'user_id' in dict_data and 'created_at' in dict_data and dict_data['user_id'] and dict_data['created_at']:
    if dict_data['created_at']< '2013-01-01 00:00:00':
      return 'drop'
    else:
      return 'yes'
  #数据是微博关注数据
  if 'fid' in dict_data and dict_data['fid']:
    return 'yes'
  return 'no'

def read_table_schema(table_name):
  '''
  func:读取表结构,数据填入全局变量table_schema
  param:
        table_name:表的名称
  '''
  global table_schema

  weibo_schema = open('/laiseek/weibo_db/config/%s.schema'%table_name,'r').read()
  regex = re.compile("""\"(.*?)\":{(.*?)}""",re.S)
  schemas = regex.findall(weibo_schema)
  for schema in schemas:
    colm_name,info = schema
    table_schema[table_name][colm_name] = {}
    info = info.replace('\n','')
    info = info.replace('"','')
    infos = info.split(',')
    for infobox in infos:
      key,value = infobox.strip().split(":")
      table_schema[table_name][colm_name][key] = value

if __name__ == "__main__":
  pass
else:
  read_table_schema('weibo_user')#读入表结构
  read_table_schema('weibo_data')#读入表结构
  read_table_schema('weibo_follow')#读入表结构
