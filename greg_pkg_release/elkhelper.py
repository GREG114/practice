import datetime
from dateutil.parser import parse
import requests
import pytz
import json

class elkhelper():
    '''
    elk操作类
    整体不做异常处理,请在使用时单独处理


    20220509研究根据条件更新数据
    get sh-market-seo/_search
    {
    "query":{
        "bool":{
        "must_not":{
            "exists":{
            "field":"排名"
            }
        }
        }  
    }
    }


    post sh-market-seo/_update_by_query
    {   
    "script": {
            "source": "ctx._source['排名']=999"
        },
    "query":{
        "match":{
        "_id":"IJngTH4BwNWUzoUlgfZc"
        }  
    }
    
    }


    '''

    def __init__(self,host,auth=("username","password")):
        '''
        host:服务器地址及端口，格式："http://xxxxxxx:9200"
        auth:账号信息，格式： ('username','password')
        '''
        self.host=host
        self.auth=auth
        self.headers= {'Content-Type': 'application/json'}

    #删除特定文档，需要索引和_id
    def delete(self,index,id):
        url = '{}/{}/_doc/{}'.format(self.host,index,id)
        req = requests.delete(url,auth=self.auth,verify=False)        
        return json.loads(req.text)

    #日期时间格式处理
    def Strfelktime(self,dt):
        '''
        将字符串格式的时间日期转换成elk能够识别的格式,含北京时区
        '''
        if type(dt)==datetime.date:
            timestr=datetime.date.strftime(dt,'%Y-%m-%dT%H:%M+08:00')
            return timestr
        if type(dt)==str:
            dt=parse(dt)        
        if dt.tzname() == 'UTC':
            timestr=datetime.datetime.strftime(dt,'%Y-%m-%dT%H:%M+00:00')
        else:
            timestr=datetime.datetime.strftime(dt,'%Y-%m-%dT%H:%M+08:00')
        return timestr
    #单条数据录入，body中需要有id字段，将自动作为_id处理
    def put(self,index,obj):
        '''
        录入elk的方法
        host=主机地址[含端口号]
        index=索引名称
        obj=要录入elk的字典对象,要求有'id'键值
        '''
        data = json.dumps(obj,ensure_ascii=True)
        url='{}/{}/_doc/{}'.format(self.host,index,obj['id'])        
        req=requests.put(url,auth=self.auth,headers=self.headers,data=data.encode(),verify=False)
        result = json.loads(req.text)
        return result
    #批量录入数据到es，目前仅支持同一个索引
    def bulk(self,index,dl):
        reqtext=''
        for obj in dl:
            o1={ "index" : { "_index" : index, "_id" :obj['id'] } }
            o2=obj
            d1 = json.dumps(o1)
            d2=json.dumps(o2)
            d3=d1+'\n'+d2+'\n'
            reqtext+=d3
        ss=requests.post(self.host+'/_bulk',auth=self.auth,headers=self.headers,data=reqtext.encode(),verify=False)  
        return json.loads(ss.text)
    #根据索引和ID获取单条数据
    def getdoc(self,index,id):
        url = '{}/{}/_doc/{}'.format(self.host,index,id)
        req = requests.get(url,auth=self.auth,verify=False)
        res = json.loads(req.text)
        return res
    #单页查询,最大支持10000条数据
    def searchdoc(self,querystr):
        '''
        单页查询,最大支持10000条数据
        querystr直接写索引名加搜索字符串,如下
        index/_search?size=xxxx?q=xxxx:xxx                        
        '''
        url = '{}/{}'.format(self.host,querystr)
        req = requests.get(url,auth=self.auth,verify=False)
        res =json.loads(req.text)
        if not 'error' in res:
            result = list(x for x in res['hits']['hits'])
            return result
        else:
            return []
    #以当前时间获取kibana支持的时间戳字符串
    def get_es_stamp(self):
        return datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%dT%H:%M+08:00')
    #单字段匹配查询
    def query(self,index,field,str,size=8000):
        body={
        "query":{
            "wildcard":{field+".keyword": {"value":"*{}*".format(str)}}
        }
        }
        url = '{}/{}/_search?size={}'.format(self.host,index,size)
        data = json.dumps(body,ensure_ascii=True)
        res = requests.post(url,headers=self.headers,auth=self.auth,data=data.encode(),verify=False)
        result = json.loads(res.text)
        data = result['hits']['hits']
        return data
    #索引删除
    def idxDel(self,idx):
        url = '{}/{}'.format(self.host,idx)
        res = requests.delete(url,headers=self.headers,auth=self.auth,verify=False)
        result = json.loads(res.text)
        return result
    #单字段匹配删除
    def deletebyquery(self,index,field,str):
        body={
            "query":{
                "wildcard":{field+".keyword": {"value":"*{}*".format(str)}}
            }
        }
        url = '{}/{}/_delete_by_query'.format(self.host,index)
        data = json.dumps(body,ensure_ascii=True)
        res = requests.post(url,headers=self.headers,auth=self.auth,data=data.encode(),verify=False)
        result = json.loads(res.text)
        return result
    #翻页获取数据
    def searchAll(self,querystr):
        '''
        全量数据查询,当所需要的数据量大于10000条时使用
        querystr直接写索引名加搜索字符串,如下
        index/_search?size=xxxx?q=xxxx:xxx   
        '''
        url = '{}/{}&scroll=1m'.format(self.host,querystr)
        req = requests.get(url,auth=self.auth,verify=False)
        result =json.loads(req.text)
        if 'hits' in result:
            data = list(x['_source'] for x in result['hits']['hits'])
        else:
            return []

        sid = result['_scroll_id']
        total = result['hits']['total']['value']
        while len(data)<total:
            url='{}/_search/scroll?scroll_id={}&scroll=1m'.format(self.host,sid)
            req=requests.get(url,auth=self.auth,verify=False)
            result=json.loads(req.text)
            data+=list(x['_source'] for x in result['hits']['hits'])
        return data
    #巨量数据时翻页批量录入，方法同bulk
    def bulkbypage(self,data,limit):
        page = int(len(data)/limit)
        if len(data)%limit !=0:
            page+=1
        for i in range(0,page):
            req = data[i*limit:][:limit]
            res = json.loads(self.bulk('sh-crm-incomes',req))
            print(res['errors'])
