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
account = 8

#构造帐号字典
account_dict = {1:5235936071,2:5236922307,3:5253042143,4:5253042143,5:5261818443,6:5251837729,7:5262542524,8:5262424390,9:5264006794,10:5263378520,
11:5263378718,12:5263749144,13:5264445452,14:5264008831,15:5264825720,16:5265129775,17:5264272886,18:5264710847,19:5264711149,20:5264883674,21:5266912391,22:5266449212,23:5266643778,
24:5266644270,25:5266644533}    





countNum = 0 #记录抓取用户的个数
countTag = 0 #断点续抓的位置


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



def parse_contents(content, uid):
	global countNum
	global dict_all
	global t1
	global account
	dict_cache = {}
	content1 = re.findall(r'<!-- 基本信息 -->(.*)</script>',''.join(content)) #抓普通用户信息
	if content1 == []:
		content2 = re.findall(r'<legend>简介(.*?)</script>',''.join(content))#抓企业,若无简介信息,直接返回
		if content2 == []:	
			return 0
		if not '联系方式' in content:#不抓没有联系方式的企业
			return 1
		print '抓企业'
		if '联系方式' in content:
			content3 = re.findall(r'<legend>联系方式(.*?)</script>',''.join(content))
			content1 = '简介'+''.join(content2) + '联系方式'+''.join(content3)
		else:
			content1 = '简介'+''.join(content2)
		content1 = re.sub(r'<(S*?)[^>]*>.*?|<.*? />|&quot;','',''.join(content1))
		content1 = re.sub(r'\\t|\\r|\\n',' ',content1)
		content1 = ','.join(content1.split())
		content1 = re.sub(',"}',',&',content1)
		content1 = re.sub('\\\\/','/',content1)
		if '联系方式' in content1:
			content1 =re.sub('联系方式','&联系方式',content1)
		dict_cache['简介'] = ''.join(re.findall('简介,(.*?),展开',content1)) # ?表示最小匹配
		if '联系方式' in content1:
			dict_cache['联系方式'] = ''.join(re.findall('联系方式,(.*?),&',content1))
		
	else:	
		age = re.findall(r'>生日<|>教育信息<|>工作信息<',''.join(content1)) #若无这些反应普通用户年龄的信息,则直接返回
		if age == []:
			return 1
		print '抓个人'
		content1 = re.sub(r'<(S*?)[^>]*>.*?|<.*? />','',''.join(content1))# 去掉html中的格式符
		content1 = re.sub(r'\\t|\\r|\\n',' ',content1)
		content1 = ','.join(content1.split())
		content1 = re.sub(',"}',',&',content1)
		content1 = re.sub('\\\\/','/',content1)
		if '联系信息' in content1:
			content1 =re.sub('联系信息','&联系信息',content1)
		if '工作信息' in content1:
			content1 =re.sub('工作信息','&工作信息',content1)
		if '教育信息' in content1:
			content1 =re.sub('教育信息','&教育信息',content1)
		if '标签信息' in content1:
			content1 =re.sub('标签信息','&标签信息',content1)
		#if '基本信息' in content1:
		dict_cache['基本信息'] = ''.join(re.findall('基本信息,(.*?),&',content1)) # ?表示最小匹配
		if '联系信息' in content1:
			dict_cache['联系信息'] = ''.join(re.findall('联系信息,(.*?),&',content1))
		if '工作信息' in content1:
			dict_cache['工作信息'] = ''.join(re.findall('工作信息,(.*?),&',content1))
		if '教育信息' in content1:
			dict_cache['教育信息'] = ''.join(re.findall('教育信息,(.*?),&',content1))
		if '标签信息' in content1:
			dict_cache['标签信息'] = ''.join(re.findall('标签信息,(.*?),&',content1))
	try:	
		dict_all[uid] = dict_cache
		countNum +=1
		print '已抓取粉丝信息个数(%d):'%(account),countNum #显示是第几个帐号抓的
	except Exception,e:
		print e

	dict_all = json.dumps(dict_all, encoding='UTF-8', ensure_ascii=False) #将字典数据转为中文格式
	f = open("shanghai1age7",'a')
	f.write(dict_all+'\n')
	f.close()
	dict_all = {}
	time.sleep(2)
	return 1
	



#解析html
'''
name='SH_V1.html'
name=unicode(name,'utf-8')
fr=open(name,'r')#读取存储的文件
html_doc=fr.read()
fr.close()
html_doc = html_doc.decode('utf-8', 'ignore')
textcontents=html_doc.encode("utf-8").decode('unicode_escape').encode("utf-8").replace("\\", "")
name1='SH_V2.html'
fw=open(name1,'w')
fw.write(textcontents)
fw.close()
exit()
'''

	
login()  #登录
headers = {'User-Agent':'Mozilla/5.0 (X11; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0'}
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
keys = open("shanghai1",'r').readlines()[place:] #从断点处开始抓取
dict_all = {}
queue = Queue()
for key in keys:
	queue.put(key)
t1 = time.time()
j = 0

while not queue.empty():

	key = queue.get()
	uid = key.split()[0]
	#print type(uid)
	try:
		url = "http://weibo.com/p/100505"+ uid +"/info?from=page_100505&mod=TAB#!/p/100505"+ uid + "/info?from=page_100505&mod=TAB#place"#拆分再合并网址#抓个人
		req = urllib2.Request(url,headers = headers)
		content = urllib2.urlopen(req).read()
		countTag +=1
		print '已访问uid个数-------------------------',countTag,time.ctime()
		f = open('bp_log','w')
		f.write(str(countTag)+'\n') #记录总共抓取的个数
		f.close()
		time.sleep(1)
		
		'''
		f = open('sp0','a')
		f.write(content)
		f.close()
		exit()
		'''
		#if '访问受限 新浪微博-随时随地分享身边的新鲜事儿' in content: '新浪通行证'
		'''
		if '访问受限' or '你的行为有些异常' in content:
			print '帐号解冻!'
			t2 = time.time() - t1
			print '访问时间:',t2
			f = open('冻.txt','w')
			f.write(str(t2)+'\n'+content+'\n')
			f.close()
			exit()
		'''
		if '基本信息' not in content:
			f = open('个人.txt','a') #记录异常网页
			f.write(content+'\n')
			f.close()
			url = "http://weibo.com/p/100606"+ uid +"/info?from=page_100606&mod=TAB#!/p/100606"+ uid + "/info?from=page_100606&mod=TAB#place"#拆分再合并网址#抓企业
			req = urllib2.Request(url,headers = headers)
			content = urllib2.urlopen(req).read()
	except Exception,e:
		print e	
	if not parse_contents(content,uid):
		print '抓取空'
		j +=1
		if j>=10: #如果连续10次抓取均为空,则认为帐号被封,自动换下一个帐号抓取
			t3 = time.time() - t1
			f = open('冻结.txt','w')
			f.write('This time visit:'+str(t3)+'\n')
			f.close()
			account +=1
			j = 0 #重新赋值为0
			if account <= account_dict.keys()[-1]: #account_dict.keys()[-1]为最大帐号数
				time.sleep(random.randint(1,10)) #休息随机秒后,换另一个帐号登录
				login()
			else:
				account = 1 #循环抓
				time.sleep(random.randint(1,10)) 
				login()
	else:
		j = 0


