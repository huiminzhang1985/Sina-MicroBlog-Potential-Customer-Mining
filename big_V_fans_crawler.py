#!/usr/bin/env python
#encoding: utf-8

import threading
import MySQLdb
import os 
import re
import time
from weibo import APIClient
from Queue import Queue
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


#***************func:获取大V粉丝信息
	def Fan_Crawler_Info(self,expires,vid):
		info = expires['users']
		if info == []:
			#print expires['total_number']
			return ''

		for info_one in info:
			info_one['uid'] = info_one['id']
			del info_one['id']
			#print info_one['uid']
			#new user
			if cache.get(info_one['uid'],0) == 0:
				#print 'new'
#			if info_one['verified'] and info_one['verified_type'] > 0:
#				continue
				if info_one['friends_count'] > info_one['followers_count'] * 10:
					continue
				if 'verified_type' not in info_one:

					info_one['verified_type'] = ''

				info_one['tag'] = ''
				try:
					info_one['tag'] = self.Tag_Info(client.tags.get(uid=info_one['uid']))
				except Exception,e:
					print 'tag:',e		
				info_one['created_at'] = self.ChangeTime(info_one['created_at'])

				description = info_one['description']
				utf8mb4_set = re.compile(r'\xF0[\x90-\xBF][\x80-\xBF]{2}| [\xF1-\xF3][\x80-\xBF]{3}|\xF4[\x80-\x8F][\x80-\xBF]{2}')
				description = description.encode('utf8')		
				match = utf8mb4_set.search(description)
 				if match:
    					description = re.sub(utf8mb4_set,'',description)	
				info_one['description'] = description
				lock.acquire()
				bpoint = db_api.put_weibo_data(info_one)
				lock.release()
				#开始插入数据库时,把抓取大V的个数写入日志文件bp_log中
				if bpoint == 'start insert':
					f = open('bp_log','w')
					f.write(str(Count.count)+'\n')
					f.close()
					#print '*****************************************************'
			
				post_time = int(time.time())
				cache.set(info_one['uid'],0,post_time)
			#把大V的uid和其粉丝的uid写入数据库
			f_v_uid = {}
			f_v_uid['vid'] = vid
			f_v_uid['fid'] = info_one['uid']
			lock.acquire()
			bpoint = db_api.put_weibo_data(f_v_uid)
			lock.release()
			#开始插入数据库时,把抓取大V的个数写入日志文件bp_log中
			if bpoint == 'start insert':
				f = open('bp_log','w')
				f.write(str(Count.count)+'\n')
				f.close()
				#print '*****************************************************'

		return 1



#***************func:获取大V信息
		
	def Crawler_Info(self,expires,screenname):
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
		lock.acquire()
		bpoint = db_api.put_weibo_data(info_one)
		lock.release()
		#开始插入数据库时,把抓取大V的个数写入日志文件bp_log中
		if bpoint == 'start insert':
			f = open('bp_log','w')
			f.write(str(Count.count)+'\n')
			f.close()
			#print '*****************************************************'

		k=0
		i=0 #控制抓取异常的次数
		next_cursor = 0
		#print info_one['uid']
		while next_cursor < 5000:
			try:
				expires = client.friendships.followers.get(uid=info_one['uid'],count=200,cursor=next_cursor,trim_status=0)
				#if k==0:
					#print expires['total_number']
				lastTime = sina.Fan_Crawler_Info(expires,info_one['uid'])
				#如果expires['users']为空,并且expires['total_number']为空,说明此时已经达到抓取上限,否则重复抓取,最大重复抓取次数为3次
				if lastTime is '':
					if expires['total_number'] is not 0:
						i +=1
						if i>2:
							break
						continue
					else:
						break
				print next_cursor
				next_cursor +=200
				#print k
				k+=200		

			except Exception,e:
				if '10022' in str(e):
					rateLimit = client.account.rate_limit_status.get()
					print 'remaining_ip_hits:%d reset_time_in_seconds:%d\n'%(rateLimit['remaining_ip_hits'],rateLimit['reset_time_in_seconds'])
					time.sleep(rateLimit['reset_time_in_seconds'])
					continue
				#如果是参数错误或者用户不存在,则直接跳出本循环
				if '10008' in str(e) or '20003' in str(e):
					break
				print 'followers:',e
				k+=200
				next_cursor +=200
		#记录已抓大V个数
		lock.acquire()
		Count.count +=1
		print 'count-------------------------------',Count.count
		lock.release()
		



#获取大V信息
class threadv(threading.Thread):

	def run(self):
		#依次取出每个大V昵称
		while not queue.empty():
			key = queue.get()
			#screenname = key.strip()
			screenname = key.split()[0]
			print 'DV_screenName:%s\n'%(screenname)
			try:
				expires = client.users.show.get(screen_name=screenname)
				lastTime = sina.Crawler_Info(expires,screenname)

			except Exception,e:
				print e

		db_api.final()

		print 'Thread:(%s) Time:%s\n'%('ok',time.ctime())

#模拟静态变量
class Count():
	count = 0


def ThreadV():
	global lock
	global queue
	global sina
	lock = threading.Lock()
	queue = Queue()
	sina = sina_data()
	#程序开始运行时,统计服务器还剩余的remaining_ip_hits数和reset_time_in_seconds数,详细信息见http://open.weibo.com/wiki/Account/rate_limit_status
	rateLimit = client.account.rate_limit_status.get()
	print 'remaining_ip_hits:%d reset_time_in_seconds:%d\n'%(rateLimit['remaining_ip_hits'],rateLimit['reset_time_in_seconds'])
	time.sleep(2)
	#bp_log为记录断点日志文件
	if not os.path.exists('/home/mii/weibo_crawler/lijun_thread/bp_log'):
		place = 0
		f = open('/home/mii/weibo_crawler/lijun_thread/bp_log','w')
		f.close()
	elif len(open('/home/mii/weibo_crawler/lijun_thread/bp_log').read()) == 0:
		place = 0
	else:
		place = int(open('bp_log').read().strip())
		Count.count = place
	#从断点处开始获取大V昵称,并放入队列queue中
	keys = open('beijing','r').readlines()[place:]
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


