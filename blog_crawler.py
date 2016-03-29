#!/usr/bin/env python
#encoding: utf-8

import threading
import MySQLdb
import time
from weibo import APIClient
from Queue import Queue
import urllib
import types
import sys
import re
import os
sys.path.append('/laiseek/weibo_db/')
import db_api
import uid_cache
#import python_pos



app_key = '868309352'
app_secret = 'e3b6d0f35110a6ab7675c0184b68bf42'
callback_url = 'http://222.44.14.9'
access_token = '2.00jZ2GqCnYTUdCccbbff88dehhw6YC'
expires_in = '1406055600'
#access_token = '2.00cj7PWDHo5okD939e540e70vD3vhC'
# 上面的语句运行一次后，可保存得到的access token，不必每次都申请
client = APIClient(app_key=app_key, app_secret=app_secret, redirect_uri=callback_url)
#	access_token, expires_in = get_access_token(app_key, app_secret, callback_url)
client.set_access_token(access_token, expires_in)
cache = uid_cache.ThreadCache()
utf8mb4_set = re.compile(r'\xF0[\x90-\xBF][\x80-\xBF]{2}| [\xF1-\xF3][\x80-\xBF]{3}|\xF4[\x80-\x8F][\x80-\xBF]{2}')	
class sina_data:
	def __init__(self):
		self.__lastTime = 0  # int64
# 		self.postag = python_pos.CRFPOS_ON_MMSEG("/home/lexxe/postag/libs/config/seg/segmentation.config","/home/lexxe/postag/libs/config/pos/pos.config")
# 		self.num = 1
# 		self.snum = 1

	
#	def pos(self,string):
#		result = ''
#		try:
#			result = self.postag.postag(string)
#		except:
#			return ''
#		rtn = ''
#		word = result[1].split('\t')
#		pos = result[0].split('\t')
#		for i in range(0,len(word)):
#			rtn = rtn + word[i] + '/' + pos[i] + ' '
#		return rtn

	def ChangeTime(self,Time):
		Time = Time.split(' ')
		year = Time[5]
		hour = Time[3]
		date = Time[2]
		month = self.ChangeMonth(Time[1])
		s = year+'-'+month+'-'+date+' '+ hour
#		print s
		return s

		
	def ChangeUnixTime(self,Time):
		Time = Time.split(' ')
		year = Time[5]
		hour = Time[3]
		date = Time[2]
		month = self.ChangeMonth(Time[1])
		s = year+'-'+month+'-'+date+' '+ hour
#		print s
		tt = time.strptime(s, "%Y-%m-%d %H:%M:%S")
		t = int(time.mktime(tt))
		return t		
		
	def ChangeMonth(self,month):
		Mon = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
		return Mon[month]
		
	def Crawler_Info(self,expires):
		info = expires['statuses']
#		print len(info)
		if info == []:
			print expires['total_number']
			return ''
		for info_one in info:
			m_id = info_one['mid']
			info_one['created_at'] = self.ChangeTime(info_one['created_at'])
			info_one['user_id'] = info_one['user']['id']
			geo = None
#			print info_one['geo']
			try:
				geo=                info_one['geo']
			except Exception,e:
				print e
			if geo:
				try:
					geo = eval(str(geo))
					info_one['latitude'] = geo['coordinates'][0]
					info_one['longitude'] = geo['coordinates'][1]
				except Exception,e:
					print e
			#去除emoji表情
    			info_one['text'] = re.sub(utf8mb4_set,'',info_one['text'].encode('utf8'))					
#			print m_id
			#db_api.put_weibo_data(info_one)
			bpoint = db_api.put_weibo_data(info_one)
			#出现timed out,断开程序                       
			if bpoint == 'stop':
                       	 exit()
			#开始插入数据库时,把抓取大V的个数写入日志文件bp_statuses_log中
			if bpoint == 'start insert':
				f = open('bp_statuses_log','w')
				f.write(str(Count.count)+'\n')
				f.close()	
		return m_id
		
	def User_Info(self,expires):
		info_one = expires
		try:
			m_created_at= info_one['status']['created_at']
			m_created_at = self.ChangeUnixTime(m_created_at)
			
			return m_created_at
		except Exception,e:
			print e
			return ''	
		





def get_access_token(app_key, app_secret, callback_url):
    client = APIClient(app_key=app_key, app_secret=app_secret, redirect_uri=callback_url)
    # 获取授权页面网址
    auth_url = client.get_authorize_url()
    print auth_url
    # 在浏览器中访问这个URL，会跳转到回调地址，回调地址后面跟着code，输入code
    code = raw_input("Input code:")
    r = client.request_access_token(code)
    access_token = r.access_token
    # token过期的UNIX时间
    expires_in = r.expires_in
    print 'access_token:',access_token
    print 'expires_in:', expires_in

    return access_token, expires_in
def Time():
	now = time.time()
	format = '%H-%M-%S'
	ctime = time.strftime(format,time.localtime(now))
	return ctime

def HandleTime(ftime,ctime):
	f = ftime.split('-')
	c = ctime.split('-')
	seconds = 60 - int(f[2]) + int(c[2])
	if int(c[1]) - int(f[1]) > 0:
		minutes = int(c[1]) - int(f[1]) - 1
		hours   = int(c[0]) - int(f[0])
	else:
		minutes = 60 - int(f[1]) + int(c[1]) - 1
		hours   = int(c[0]) - int(f[0]) - 1
	time = hours * 60*60 + minutes * 60 + seconds
	time = int(time)
	return time

class threadv(threading.Thread):
#	app_key = '868309352'
#	app_secret = 'e3b6d0f35110a6ab7675c0184b68bf42'
#	callback_url = 'http://222.44.14.9'
#	access_token = '2.00cj7PWD0Yv1lw1c82614622AptqRC'
#	expires_in = '1396984841'
# 上面的语句运行一次后，可保存得到的access token，不必每次都申请
#	client = APIClient(app_key=app_key, app_secret=app_secret, redirect_uri=callback_url)
#	access_token, expires_in = get_access_token(app_key, app_secret, callback_url)
#	client.set_access_token(access_token, expires_in)
#	print access_token, expires_in
#	exit()
	#sina = sina_data()
	#keys = sina.getKeys()
# 使用获得的OAuth2.0 Access Token调用API
#print client.get.statuses__user_timeline()
	#screen_names = open('shanghai3','r').readlines()
#	print uids
#	abc = open('error.log','a')
	def run(self):
		#依次取出每个大V昵称
		while not queue.empty():
			key = queue.get()
			screenname = key.strip()
			t = cache.getAll(int(screenname))
			status_time = int(t[1])
			print screenname
#			status_time = 0 #第一次抓 初始化一次
			if status_time == 0:
				try:
					expires = client.users.show.get(uid=screenname)
					stime = sina.User_Info(expires)
				except Exception,e:
					print e
					continue
				if stime == '':
					print 'no status'
					continue
				if stime > int(time.time()) - 3600*24*30:
					print 'active'
			
					try:
						expires = client.statuses.user_timeline.get(uid=screenname,count=100,page=1)
					except Exception,e:
						print e
						continue
					lastTime = sina.Crawler_Info(expires)
				cache.set(int(screenname),1,stime)
		db_api.final()				
		
	

		
def ThreadV():
	global queue
	global sina
	queue = Queue()
	sina = sina_data()
	#程序开始运行时,统计服务器还剩余的remaining_ip_hits数和reset_time_in_seconds数,详细信息见http://open.weibo.com/wiki/Account/rate_limit_status
	rateLimit = client.account.rate_limit_status.get()
	print 'remaining_ip_hits:%d reset_time_in_seconds:%d\n'%(rateLimit['remaining_ip_hits'],rateLimit['reset_time_in_seconds'])
	time.sleep(2)
	#bp_statuses_log为记录断点日志文件
	if not os.path.exists('/home/mii/weibo_crawler/lijun_thread/bp_statuses_log'):
		place = 0
		f = open('/home/mii/weibo_crawler/lijun_thread/bp_statuses_log','w')
		f.close()
	elif len(open('/home/mii/weibo_crawler/lijun_thread/bp_statuses_log').read()) == 0:
		place = 0
	else:
		place = int(open('bp_statuses_log').read().strip())
		Count.count = place
	#从断点处开始获取大V昵称,并放入队列queue中
	keys = open('shanghai3','r').readlines()[place:]
	for key in keys:
		queue.put(key)
	#开启多线程
	n = 5
	for i in range(n):
		t = threadv()
		t.start()
		#t.join()	


if __name__ == '__main__':
	ThreadV()




