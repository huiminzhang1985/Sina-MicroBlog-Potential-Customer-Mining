#encoding: utf-8
# -*- coding: utf-8 -*-
import sys
reload(sys) 
sys.setdefaultencoding( "utf-8" )

from BeautifulSoup import BeautifulSoup
from Queue import Queue 
#获取热门话题
import urllib2,string
import urllib
import cookielib
import base64
import re
import os
import json
import rsa
import binascii
import time
import pickle
import threading
import random

#开始抓取的帐号
account = 26

#构造帐号字典
account_dict = {26:5269916535,27:5270235194,28:5269325396,29:5269325946,30:5269326377,31:5270239359,32:5270239566,33:5269329231,34:5269922918,35:5270240392} 

file_total = ['shanghai1_age_order_unique_morePrecise00_12.txt','shanghai1_age_order_unique_morePrecise13_18.txt','shanghai1_age_order_unique_morePrecise19_23.txt',
'shanghai1_age_order_unique_morePrecise24_30.txt','shanghai1_age_order_unique_morePrecise31_39.txt','shanghai1_age_order_unique_morePrecise40_70.txt']


cj = cookielib.LWPCookieJar()
cookie_support = urllib2.HTTPCookieProcessor(cj)
opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)
urllib2.install_opener(opener)


#获取servertime和nonce值
def get_servertime():
    global account
    global account_dict
    url = 'http://login.sina.com.cn/sso/prelogin.php?entry=weibo&callback=sinaSSOController.preloginCallBack&su=&rsakt=mod&client=ssologin.js(v1.4.5)&_=%d'%(account_dict[account])

    data = urllib2.urlopen(url).read()
    p = re.compile('\((.*)\)')
    try:	#解析返回的json数据，获取servertime和nonce等值
        json_data = p.search(data).group(1)
        ###print json_data
        data = json.loads(json_data)
        servertime = str(data['servertime'])
        pubkey = data['pubkey']
        nonce = data['nonce']
        rsakv = data['rsakv']
 
        return servertime, nonce, pubkey, rsakv
    except:
        print 'Get severtime error!'
        return None

#密码解密
def get_pwd(pwd, servertime, nonce, pubkey):
    '''
    pwd1 = hashlib.sha1(pwd).hexdigest()
    pwd2 = hashlib.sha1(pwd1).hexdigest()
    pwd3_ = pwd2 + servertime + nonce
    pwd3 = hashlib.sha1(pwd3_).hexdigest()
    return pwd3
    '''
    #print int('05k8rMAtt4Sru45CqbG7',16)
    rsaPublickey = int(pubkey,16)
    key = rsa.PublicKey(rsaPublickey,65537)
    #print key
    message = str(servertime)+'\t'+str(nonce)+'\n'+str(pwd)  
    pwd = rsa.encrypt(message,key)
    pwd = binascii.b2a_hex(pwd)
    ###print 'The key is:'+pwd
    return pwd

#用户名解密
def get_user(username):
    username_ = urllib.quote(username)
    username = base64.encodestring(username_)[:-1]
    return username

def login():  # 模拟登录程序
    postdata = {
    'entry': 'weibo',
    'gateway': '1',
    'from': '',
    'savestate': '7',
    'userticket': '1',
    'ssosimplelogin': '1',
    'vsnf': '1',
    'vsnval': '',
    'su': '',
    'service': 'miniblog',
    'servertime': '',
    'nonce': '',
    'pwencode': 'rsa2', #'wsse',
    'sp': '',
    'encoding': 'UTF-8',
        ####
    'prelt':'115',
    'rsakv':'',
    ####
    'url':'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack',
        #'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack',
    'returntype': 'META'
}
    global account
    username = 'lasclocker%d@163.com'%(account)
    pwd = '1161895575'
    url = 'http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.5)'
    try:#主要获取servertime和nonce这两个随机的内容
        servertime, nonce, pubkey, rsakv = get_servertime()
    except:
        return
    #global postdata
    postdata['servertime'] = servertime
    postdata['nonce'] = nonce
    postdata['rsakv']= rsakv
    postdata['su'] = get_user(username)#对用户名进行加密
    postdata['sp'] = get_pwd(pwd, servertime, nonce, pubkey)#对密码进行加密
    postdata = urllib.urlencode(postdata)
    #headers = {'User-Agent':'Mozilla/5.0 (X11; Linux i686; rv:8.0) Gecko/20100101 Firefox/8.0'}#设置post头部，根据不同的应用平台进行设定
    headers = {'User-Agent':'Mozilla/5.0 (X11; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0'}
    #headers = {'User-Agent':'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)'}
    req  = urllib2.Request(
        url = url,
        data = postdata,
        headers = headers
    )
   
    result = urllib2.urlopen(req)
    
    text = result.read()
    p = re.compile('location\.replace\(\'(.*?)\'\)')
    try:
        login_url = p.search(text).group(1)
        ###print login_url
        urllib2.urlopen(login_url)
        print "Login successful!"
    except:
        print 'Login error!'

	
login()  #登录
headers = {'User-Agent':'Mozilla/5.0 (X11; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0'}
#url = 'http://weibo.com/p/1005053312179975/huati?from=page_100505&mod=TAB#!/p/1005053312179975/huati?from=page_100505&mod=TAB#place'
#url = 'http://weibo.com/p/1005051287877200/huati?Pl_Core_LeftPicTextMixedGalley__63_page=1#!/p/1005051287877200/huati?Pl_Core_LeftPicTextMixedGalley__63_page=1#place'
#ls = open('huati','r').readlines()
#con1 = re.findall(r'TA 的话题(.*)热门话题榜',''.join(ls).replace('\n',' '))

for i in range(len(file_total)): #依次读取每个文件
	file_name = file_total[i]
	goal_file = 'topic_' + file_name[-9:]
	countTag = 0 #断点续抓的位置

	if not os.path.exists('bp_log'): #读取断点的位置
		place = 0
		f = open('bp_log','w')
		f.close()
	elif len(open('bp_log').read()) == 0:
		place = 0
	else:
		place = int(open('bp_log').read().strip())
		countTag = place
		place = int(open('bp_log').read().strip())
		countTag = place
	keys = open(file_name,'r').readlines()[place:] #从断点处开始抓取
	queue = Queue()
	for key in keys:
		queue.put(key)
	t1 = time.time()
	while not queue.empty():
		
		key = queue.get()
		uid = key.split('\t')[0]
		#print type(uid)
		try:
			flag = 1
			num = 1
			con = []
			while flag:
				print 'uid:num',uid,num
				url = 'http://weibo.com/p/100505' + uid + '/huati?Pl_Core_LeftPicTextMixedGalley__63_page=%d#!/p/100505'%(num) + '/huati?Pl_Core_LeftPicTextMixedGalley__63_page=%d#place'%(num)
				req = urllib2.Request(url,headers = headers)
				content = urllib2.urlopen(req).read()
				con1 = re.findall(r'TA 的话题(.*)热门话题榜',content.replace('\n',' ')) #一定要把换行符去掉
				con1 = re.findall(r'target=\'_blank\'>(.*?)<',''.join(con1)) #此处的查找条件中,用 \'代替 '
				if con1 == []:
					flag = 0
					if ('验证码' or '你的行为有些异常' or '访问受限') in content:
						f = open('帐号解冻','w')
						f.write(content + '')
						f.close()
						print '****帐号解冻****'
						countTag -= 1  #由于帐号异常导致的抓取失败不计入内
						f = open('abnormal_' + file_name[-9:],'a')
						f.write(uid + '\n')
						f.close()
						#exit()
						account +=1
						if account <= account_dict.keys()[-1]: #account_dict.keys()[-1]为最大帐号数
							time.sleep(random.randint(1,10)) #休息随机秒后,换另一个帐号登录
							login()
						else:
							account = 26 #循环抓
							time.sleep(random.randint(1,10)) 
							login()
				else:
					con += con1
				num += 1
				if not '下一页' in content: #如果此人的话题只有一页,则跳出循环.
					break
			if con == []:
				print '%s还没有参与任何话题'%(uid)
			else:
				con = ' '.join(con)
				f = open(goal_file,'a')
				f.write(con + ' ')
				f.close() 
			countTag +=1
			print '已访问uid个数------------(%d):'%(account),countTag,time.ctime()
			f = open('bp_log','w')
			f.write(str(countTag)+'\n') #记录总共抓取的个数
			f.close()
		except Exception,e:
			print e
	os.remove('bp_log') #清空读取下一个文件
