#!/usr/bin/env python
#encoding: utf-8

import threading
import MySQLdb
import re
import time
from weibo import APIClient
import sys
sys.path.append('/laiseek/weibo_db/')
import db_api
import uid_cache
reload(sys)
sys.setdefaultencoding('utf-8')

app_key = '868309352'
app_secret = 'e3b6d0f35110a6ab7675c0184b68bf42'
callback_url = 'http://222.44.14.9'
access_token = '2.00jZ2GqCnYTUdCccbbff88dehhw6YC'
#access_token = '2.00cj7PWDHo5okD939e540e70vD3vhC'
expires_in = '1406055600'
# 上面的语句运行一次后，可保存得到的access token，不必每次都申请
client = APIClient(app_key=app_key, app_secret=app_secret, redirect_uri=callback_url)
#access_token, expires_in = get_access_token(app_key, app_secret, callback_url)
#exit()
client.set_access_token(access_token, expires_in)
cache = uid_cache.ThreadCache()

class sina_data:
	def __init__(self):

		self.__num = 0
		self.__lastTime = 0  # int64

 
	def ChangeTime(self,Time):
		Time = Time.split(' ')
		year = Time[5]
		hour = Time[3]
		date = Time[2]
		month = self.ChangeMonth(Time[1])
		time = year+'-'+month+'-'+date+' '+ hour
		return time
		
	def ChangeMonth(self,month):
		Mon = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
		return Mon[month]




	def Statuses_Crawler_Info(self,expires,vid):#获取并处理博文信息
		info = expires['statuses']
#		print len(info)
		if info == []:
			print expires['total_number']
			return ''
		for info_one in info:
			m_id = info_one['mid'] #微博mid
			tt = info_one['created_at']
			tt = self.ChangeTime(tt)
			if info_one['comments_count'] > 100 and tt > '2013-01-01 00:00:00':
#				print info_one['text']
				page = 1
				while page < 11:
					print m_id
					try:
						expires = client.comments.show.get(id=m_id,count=200,page=page) #抓该微博的评论
					except Exception,e:
						print e
						page+=1
						continue
					try:
						lastTime = self.Status_Info(expires,vid)
					except Exception,e:
						print e
						page+=1
						continue
					page+= 1
		
					if lastTime is '':
						break	
		return m_id	#或者写成 return 1	
				
				
				
	
	def Status_Info(self,expires,vid):     #对评论信息处理后,把评论者写到数据库中
		tmpuid = 0
		info = expires['comments']
#		print info
		if info == []:
			#print expires['total_number']
			return ''
		#pubtime = ''
		#统计抓取大V博文评论者数目
		#lock.acquire()
		#UserSum.userSum +=len(info)
		#print '目前抓取评论者总数为:%d\n'%(UserSum.userSum)
		#lock.release()
		print 'comments start!'
		for info_one in info: 
			info_user = info_one['user']
			info_user['uid'] = info_user['id']
			del info_user['id']
			print info_user['uid']
			if cache.get(info_user['uid'],0) == 0:
				print 'new'
#			if info_user['verified'] and info_user['verified_type'] > 0:
#				continue
				if info_user['friends_count'] > info_user['followers_count'] * 10:
					continue
				if info_user['uid'] == tmpuid:
					print 'repeat'
					continue
				tmpuid	= info_user['uid']		
				if 'verified_type' not in info_user:
					info_user['verified_type'] = ''
				info_user['tag'] = ''
				try:
					info_user['tag'] = self.Tag_Info(client.tags.get(uid=info_user['uid']))
				except Exception,e:
					print 'tag:',e			
				info_user['created_at'] = self.ChangeTime(info_user['created_at'])


				#去掉大V用户博文评论者信息中description的编码为utf8mb4的部分	
				description = info_user['description']
				utf8mb4_set = re.compile(r'\xF0[\x90-\xBF][\x80-\xBF]{2}| [\xF1-\xF3][\x80-\xBF]{3}|\xF4[\x80-\x8F][\x80-\xBF]{2}')
				description = description.encode('utf8')		
				match = utf8mb4_set.search(description)
 				if match:
    					description = re.sub(utf8mb4_set,'',description)	
				info_user['description'] = description

				#把大V用户博文评论者信息中写入数据库
				db_api.put_weibo_data(info_user)	
				post_time = int(time.time())
				cache.set(info_user['uid'],0,post_time)
			#把大V的uid和其博文评论者的uid写入数据库
			f_v_uid = {}
			f_v_uid['vid'] = vid
			f_v_uid['fid'] = info_user['uid']
			db_api.put_weibo_data(f_v_uid)
			
		return info_user['created_at']				        


#***************func:获取大V信息
		
	def Crawler_Info(self,expires):
		info_one = expires
		if info_one == {}:
			return ''

		if 'verified_type' not in info_one:

			info_one['verified_type'] = ''

		info_one['tag'] = ''
		try:
			info_one['tag'] = self.Tag_Info(client.tags.get(uid=info_one['id']))
		except Exception,e:
			print 'tag:',e		
		info_one['created_at'] = self.ChangeTime(info_one['created_at'])
		info_one['uid'] = info_one['id']
		del info_one['id']
		#print info_one['uid']

                #去掉大V信息中description的编码为utf8mb4的部分
		description = info_one['description']
		utf8mb4_set = re.compile(r'\xF0[\x90-\xBF][\x80-\xBF]{2}| [\xF1-\xF3][\x80-\xBF]{3}|\xF4[\x80-\x8F][\x80-\xBF]{2}')
		description = description.encode('utf8')		
		match = utf8mb4_set.search(description)
 		if match:
    			description = re.sub(utf8mb4_set,'',description)	
		info_one['description'] = description
		#把大V抓取时间(post_time)写入缓存(uid_cache)
		post_time = int(time.time())
		cache.set(info_one['uid'],0,post_time)
		#把大V信息写入数据库
		db_api.put_weibo_data(info_one)

		#抓取大V博文内容主程序
		page = 1
		while page < 21:
			print page
			try:
				expires = client.statuses.user_timeline.get(uid=info_one['uid'],count=100,page=page)

			except Exception,e:

				#如果是IP请求频次超过上限,则程序进入休眠,休眠时间为reset_time_in_seconds,详细错误信息见http://open.weibo.com/wiki/Error_code
				if '10022' in str(e):
					rateLimit = client.account.rate_limit_status.get()
					print 'remaining_ip_hits:%d reset_time_in_seconds:%d\n'%(rateLimit['remaining_ip_hits'],rateLimit['reset_time_in_seconds'])
					time.sleep(rateLimit['reset_time_in_seconds'])
					continue

				#如果是参数错误,则直接跳出本循环
				if '10008' in str(e):
					break
				print e
				page+=1
				continue
			try:			
				lastTime = sina.Statuses_Crawler_Info(expires,info_one['uid'])
			except Exception,e:
				print e
				page+=1
				continue
			if lastTime is '':
				break
			page+=1

		return 1				


		


	def Tag_Info(self,expires):
		info = expires
		k = ''
		for i in info:
			i = str(i)
			i = eval(i)
			for key in i.keys():
				if key == 'weight':
					continue
				if key == 'flag':
					continue
				k +=i[key]+','
		return k[:-1]


def StrtoUnicode(str):
	if str.find("\u") != -1:
		try:
			str2 = eval('u"' + str + '"')
		except Exception, e:
			print 'str:', '[' + str +']'
			print 'str2:', '[' + str2 +']'
			print e
		if type(str) is types.UnicodeType:
			str2 = str2.encode('utf8')
		return str2
	else:
		str2 = str.decode('utf8')
		return str2

def CurrentTime():
	now = time.time()
	format = '%Y-%m-%d'# %H:%M:%S'
	CTime = time.strftime(format,time.localtime(now))
	return CTime
def ConvertRTime(Time):
	Ptime = time.strptime(Time,'%Y-%m-%d')# %H:%M:%S')	
	ptime = time.mktime(Ptime)
	return int(ptime)

def insertTimeFun():
	now = time.time()
	format = '%Y-%m-%d,%H-%M-%S'
	Time = time.strftime(format,time.localtime(now))
	return Time

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

#定义类,模拟静态变量
#class UserSum():
#	userSum = 0

class ThreadV(threading.Thread):
	def __init__(self,keys):
		#调用父类初始化方法
		threading.Thread.__init__(self)
		self.keys = keys
		self.thread_stop = False
	def run(self):
		while not self.thread_stop:
			for key in self.keys:
				screenname = key.replace('\n','')
				print 'DV_screenName:%s\n'%(screenname)
				try:
					#加锁
					#lock.acquire()
					expires = client.users.show.get(screen_name=screenname)
					#lock.release()
					lastTime = sina.Crawler_Info(expires)

				except Exception,e:
					print e

			db_api.final()
			print 'Thread:(%d) Time:%s\n'%(5,time.ctime())

	def stop(self):
		self.thread_stop = True


#抓大V信息的线程函数
def threadVMain():
	global lock
	global sina
	#threshold_crawler = 3000
	#上锁,用于统计抓取粉丝的总数
	lock = threading.RLock()
	sina = sina_data()
	#程序开始运行时,统计服务器还剩余的remaining_ip_hits数和reset_time_in_seconds数,详细信息见http://open.weibo.com/wiki/Account/rate_limit_status
	rateLimit = client.account.rate_limit_status.get()
	print 'remaining_ip_hits:%d reset_time_in_seconds:%d\n'%(rateLimit['remaining_ip_hits'],rateLimit['reset_time_in_seconds'])
	time.sleep(2)
	#if rateLimit['remaining_ip_hits']<=threshold_crawler:
	#	print 'remaining_ip_hits:%d reset_time_in_seconds:%d\n'%(rateLimit['remaining_ip_hits'],rateLimit['reset_time_in_seconds'])
	#	time.sleep(rateLimit['reset_time_in_seconds'])
	
	#把大V信息5等分后分别加入到5个线程,之后依次开启5个线程
	keys = open('weibo_V.txt','r').readlines()
	n=5
	len_V = len(keys)/n
	t1 = []
	for i in range(n-1):
		t = ThreadV(keys[i*len_V:(i+1)*len_V])
		t1.append(t)
	t1.append(ThreadV(keys[(n-1)*len_V:]))

	for i in t1:
		i.start()
		#time.sleep(1)


if __name__ == '__main__':
	threadVMain()


