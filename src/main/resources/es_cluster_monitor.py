#!/usr/bin/python
# report elasticsearch node status to monitor
# -- run every 30s to detect LONG G1GC app stopping, try 'jmap -heap' may help node recover. 
# -- If fail after 5min, kill -9 to the process.  All these operations can be notified by email.
#coding=utf-8

import requests
import os
import httplib
import json
import io
import time
import subprocess
import smtplib
from email.mime.text import MIMEText


def send_mail(subject, content):  
    msg_from='test@qq.com' #sender
    passwd='abcdefg'#auth code from qq mailer
    msg_to='test@qq.com'  #receiver
                                
    msg = MIMEText(content)
    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to
    try:
        s = smtplib.SMTP_SSL("smtp.qq.com",465)
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print "mail success!"
    except s.SMTPException,e:
        print "mail failed"
    finally:
        s.quit()

def httpPost(data_post):
    headers = {"Content-Type": "application/json", "Accept": "text/plain","Authorization":"Basic YOUR_PWD_BASE64"}
    exception = None
    http_client = None
    try:
        try:
            http_client = httplib.HTTPConnection('127.0.0.1', 8200) #monitor es cluster!!!
            http_client.request(method="POST", url='.monitoring-es-6-*/_search', body=data_post, headers=headers)
            response = http_client.getresponse()
            if response.status == 200:
                resp = json.loads(response.read())
                return resp
            else:
                print " >>> response error: code=%d , %s" % (response.status,response.read())
        except Exception, ex:
            exception = ex
    finally:
        if http_client:
            http_client.close()
        if exception:
            print exception
def checkHealth(post):
    resp = httpPost(post)
    if resp is None:
        return
        
    #print json.dumps(resp["aggregations"]["node_name"]["buckets"][0])
    liNodes = resp["aggregations"]["node_name"]["buckets"]
    lazyNode = liNodes[0]

    t1 = lazyNode["last_update"]["value"]
    t3 = liNodes[4]["last_update"]["value"]
    ts = int((t3-t1)/1000)
    print ">>> detect node: %s  at %s, ts_delta=%s s"%(lazyNode["key"],lazyNode["last_update"]["value_as_string"],ts)
    
    if lazyNode["key"].find("-master")>0 or ts>1200 :   #dead node? skip!
       lazyNode = liNodes[1]
       ts = int((t3-lazyNode["last_update"]["value"])/1000)

    if ts>20: #30s
        print "Bad  node: %s  at %s"%(lazyNode["key"],lazyNode["last_update"]["value_as_string"])

        if ts<120 :# 30s-60s
	        postIp = post.replace("source_node.name","source_node.ip")
                ips=httpPost(postIp)
                nodes = ips["aggregations"]["node_name"]["buckets"]
                host = nodes[0]["key"]
                if nodes[0]["last_update"]["value"] != lazyNode["last_update"]["value"] :
		    host=nodes[1]["key"]  #dead node?

                cmdOutput = sendCmd(host)
		print "Jmap attach at [%s] - %s" % (host,ts)
		#if ts>60 :
                send_mail("jmap attach at [%s] in %s s" % (lazyNode["key"],ts), host+"\n"+cmdOutput)
        	#else:
		#	send_mail("Nodes changed,no more kills to %s" % lazyNode["key"],host)
                # 	print "Nodes changed,no kills send to %s!"%host
        else:
		print "no kills..."
    
        if ts>=120 and ts<270 :# 10 times alert.
            send_mail("Bad node [%s] at %s"%(lazyNode["key"],lazyNode["last_update"]["value_as_string"]),"%s s"%ts )
	    print "Send mail >>> Bad  node: %s  at %s"%(lazyNode["key"],lazyNode["last_update"]["value_as_string"])
	else:
	    if ts>=270 and ts<300:
	        cmdOutput=subprocess.check_output("pssh -H %s -i \'kill -9 $(pgrep java)\'"%host ,stderr=subprocess.STDOUT,shell=True)
	        send_mail("kill process "%host,cmdOutput)

	    print "Bad node, but no more mails to send..."
    
def sendCmd(host):
        output = ""
        try:
                output = subprocess.check_output("pssh -H %s -i \'jmap -heap $(pgrep java)\'"%host ,stderr=subprocess.STDOUT,shell=True)
		#time.sleep(2)

        except subprocess.CalledProcessError as e:
                output = "error on pssh:\n" + e.output
                print output

        return output
        # jmap -heap $(pgrep java)
        #subprocess.call('pssh -H %s -i \'kill -9 `pgrep java`\' ' % host, shell=True)

def postAggReq():
    post = '''{
  "size":0,
  "query":{
    "term":{"type":"node_stats"}
  },
  "aggs":{
     "node_name":{
       "terms":{"field":"source_node.name",
                "size":%s,
        "order" : { "last_update" : "asc" }
       }
       ,
       "aggs" : {
        "last_update":{"max":{ "field":"timestamp" }}
       }
     }
  }
}''' %('30')

    checkHealth(post)

if __name__ == '__main__':
        postAggReq()

