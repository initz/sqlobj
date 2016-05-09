# -*- coding: utf-8 -*-
import json,urllib2
from .logutil import LogUtil
from datetime import date,time,datetime
from sqlobject import *
from sqlobject.include.hashcol import HashCol
from sqlobject.sqlbuilder import *
from sqlobject.classregistry import findClass

COLUMNS={'bigint':BigIntCol,'blob':BLOBCol,'bool':BoolCol,'datetime':DateTimeCol,'date':DateCol,'time':TimeCol,'timestamp':TimestampCol,'float':FloatCol,'int':IntCol,'foreignKey':ForeignKey,'varchar':UnicodeCol,'text':UnicodeCol,'hash':HashCol}
JOIN={'singleJoin':SingleJoin,'multipleJoin':SQLMultipleJoin,'relatedJoin':SQLRelatedJoin}
ID='_id'
class ObjStyle(Style):
  def instanceAttrToIDAttr(self,attr):
    return attr+ID
  def instanceIDAttrToAttr(self,attr):
    return attr[:-len(ID)]
class Schema(SQLObject):
  class sqlmeta:
    style=ObjStyle()
class Resource:
  model=None
  name=None
  joins=None
  excCols=None
  def __init__(self,model,name=None,excCols=None,logPath='/dev/null'):
    self.model=model
    self.tableName=self.model.sqlmeta.table
    self.name=name or self.tableName
    self.joins={}
    self.logPath=logPath
    self.logger=LogUtil.getLogger(self.logPath,'sqlobj.Resource.{0}'.format(self.tableName))
    for k,v in self.model.sqlmeta.columns.items():
      if type(v)==col.SOForeignKey:
        model=findClass(v.foreignKey,self.model.sqlmeta.registry)
        fk=k[:-len(ID)]
        self.joins[fk]={'model':model}
        self.joins[fk]['joinOn']=self._createExpr('{0}.{1}'.format(fk,'id'))==self._createExpr(k)
    if excCols:
      self.excCols=excCols
  def createTable(ifNotExists=True):
    return self.model.createTable(ifNotExists=ifNotExists)
  def dropTable(ifExists=True):
    return self.model.dropTable(ifExists=ifExists)
  def toDict(self,obj,l=[]):
    if isinstance(obj,(list,sresults.SelectResults)):
      return [self.toDict(x) for x in list(obj)]
    else:
      tmp={}
      l=l+[obj.sqlmeta.table]
      for k,v in vars(type(obj)).items():
        try:
          if isinstance(v,property):
            if self.excCols and k in self.excCols:
              continue
            value=getattr(obj,k)
            bases=type(value).__bases__
            if value==None: # None value
              tmp[k]=None
            elif isinstance(value,list) and len(value)>0 and not value[0].sqlmeta.table in l: # self fk to other and other multiple join to self
              tmp[k]=self.toDict(value,l)
            elif (Schema in bases and value.sqlmeta.table in l) or (isinstance(value,list) and len(value)>0 and value[0].sqlmeta.table in l): # self join to other and other join back to self
              continue
            elif Schema in bases: # self fk to other
              tmp[k]=self.toDict(value,l)
            elif datetime in bases or date in bases or time in bases: # datetime
              tmp[k]=str(value)
            else: # normal
              tmp[k]=value
        except Exception as e:
          self.logger.exception(k,type(obj))
      tmp['id']=obj.id
      return tmp
  def _createExpr(self,column):
    if '.' in column:
      fk,column=column.split('.')
      return getattr(self.joins[fk]['model'].q,column)
    else:
      return getattr(self.model.q,column)
  def _createClause(self,param,op=None):
    tmp,join=[],{}
    for key,value in param.items():
      if key=='$not':
        tmp.append(self._createClause(value,'NOT'))
      elif key=='$or':
        tmp.append(OR(*(self._createClause(v,'OR') for v in value)))
      elif key=='$and':
        tmp.append(AND(*(self._createClause(v,'AND') for v in value)))
      else:
        if isinstance(value,dict):
          tmp2=[]
          for k,v in value.items():
            if k=='$in':
              tmp.append(IN(self._createExpr(key),v))
            elif k=='$like':
              tmp2.append(LIKE(self._createExpr(key),v))
            elif k=='$lt':
              tmp2.append(self._createExpr(key)<v)
            elif k=='$lte':
              tmp2.append(self._createExpr(key)<=v)
            elif k=='$gt':
              tmp2.append(self._createExpr(key)>v)
            elif k=='$gte':
              tmp2.append(self._createExpr(key)>=v)
            elif k=='$ne':
              tmp2.append(self._createExpr(key)!=v)
            elif k=='$eq':
              tmp2.append(self._createExpr(key)==self._createExpr(v))
          tmp.extend(tmp2)
        else:
          tmp.append(self._createExpr(key)==value)
        if '.' in key:
          join[key.split('.')[0]]=self.joins[key.split('.')[0]]['joinOn']
    if op=='NOT':
      tmp=NOT(*tuple(tmp))
    elif op=='OR':
      tmp=OR(*tuple(tmp))
    else:
      tmp=AND(*tuple(tmp))
    return AND(tmp,*tuple(join.values()))
  def _prepareData(self,data):
    tmp={}
    columns=self.model.sqlmeta.columns
    for k,v in data.items():
      if k in columns:
        try:
          col=columns[k].__class__.__name__.lower()
          if col in ['soforeignkey']:
            if k.endswith(ID):
              k=k[:-len(ID)]
            tmp[k]=int(v)
          elif col in ['sointcol']:
            tmp[k]=int(v)
          elif col in ['sobigintcol']:
            tmp[k]=long(v)
          elif col in ['sofloatcol']:
            tmp[k]=float(v)
          elif col in ['soboolcol']:
            tmp[k]=not v in [0,'0','False','false',False] #bool(v)
          elif col in ['sodatetimecol']:
            tmp[k]=datetime.strptime(v,'%Y-%m-%d %H:%M:%S')
          elif col in ['sodatecol']:
            tmp[k]=datetime.strptime(v,'%Y-%m-%d').date()
          elif col in ['sotimecol']:
            tmp[k]=datetime.strptime(v,'%H:%M:%S').time()
          elif col in ['sotimestampcol']:
            tmp[k]=datetime.strptime(v,'%Y-%m-%d %H:%M:%S')
          else:
            tmp[k]=v
        except Exception as e:
          tmp[k]=v
          self.logger.exception(k,v)
    return tmp
  def find(self,param=None,orderBy=None,limit=None,distinct=False,reversed=False):
    if param:
      param=self._createClause(param)
    return self.model.select(param,orderBy=orderBy,limit=limit,distinct=distinct,reversed=reversed)
  def findOne(self,param=None,orderBy=None,distinct=False,reversed=False):
    if isinstance(param,(int,long,str,unicode)):
      return self.model.get(int(param))
    else:
      result=self.find(param,orderBy=orderBy,distinct=distinct,reversed=reversed)
      if result.count()>0:
        return list(result)[0]
      else:
        return None #raise ValueError('{0} not found in table {1}'.format(param,self.tableName))
  def insert(self,data):
    data=self._prepareData(data)
    return self.model(**data)
  def update(self,param,data):
    data=self._prepareData(data)
    q=self.findOne(param)
    q.set(**data)
    return self.findOne(param)
  def remove(self,id):
    self.model.get(id).destroySelf()
  def findDict(self,param=None,orderBy=None,limit=None,distinct=False,reversed=False):
    try:
      res=self.toDict(self.find(param=param,orderBy=orderBy,limit=limit,distinct=distinct,reversed=reversed))
      return res
    except Exception as e:
      self.logger.exception(e)
      return None
  def findOneDict(self,param=None,orderBy=None,distinct=False,reversed=False):
    try:
      res=self.toDict(self.findOne(param))
      return res
    except Exception as e:
      self.logger.exception(param)
      return None
  def insertDict(self,data):
    try:
      res=self.toDict(self.insert(data))
      return res
    except Exception as e:
      self.logger.exception(data)
      return None
  def updateDict(self,param,data):
    try:
      res=self.toDict(self.update(param,data))
      return res
    except Exception as e:
      self.logger.exception(param,data)
      return None
  def removeDict(self,id):
    try:
      self.remove(id)
      return True
    except Exception as e:
      self.logger.exception(id)
      return False
  def count(self,param=None):
    return self.find(param).count()

class Database:
  def __init__(self,uri,logPath='/dev/null',*args,**kwargs):
    self.connection=connectionForURI(uri)
    sqlhub.processConnection=self.connection
    self.models={}
    self.logPath=logPath
    self.logger=LogUtil.getLogger(self.logPath,'sqlobj.Database')
  def close(self):
    sqlhub.processConnection.close()
  def resourceFromSchema(self,resource):
    if isinstance(resource,(list,tuple)):
      for rsc in resource:
        self.resourceFromSchema(rsc)
    else:
      if type(resource)==declarative.DeclarativeMeta:
        resource=Resource(resource,logPath=self.logPath)
      setattr(self,resource.name,resource)
      self.models[resource.name]=resource
  def _jsonSqlmeta(self,tableName,fromDatabase=False):
    return {'sqlmeta':type('sqlmeta',(sqlmeta,),{'table':tableName,'style':ObjStyle(),'fromDatabase':fromDatabase})}
  def _jsonColumns(self,columns):
    tmp={}
    for column in columns:
      colType=column.pop('type')
      colName=column.pop('name')
      attrDefault='default' in column
      attrLength='length' in column
      attrNotNull='notNull' in column
      if not 'dbName' in column:
        #column['dbName']=colName.lower()
        if colType=='foreignKey':
          column['dbName']+=ID
      if attrLength:
        column['length']=int(column['length'])
      if attrNotNull and type(column['notNull'])==str:
        column['notNull']=json.loads(column['notNull'].lower())
      if colType=='datetime' and ((not attrDefault and attrNotNull and column['notNull']) or (attrDefault and column['default']=='now')):
        column['default']=datetime.now
      elif colType=='date' and ((not attrDefault and attrNotNull and column['notNull']) or (attrDefault and column['default']=='now')):
        column['default']=datetime.now().date
      elif colType=='time' and ((not attrDefault and attrNotNull and column['notNull']) or (attrDefault and column['default']=='now')):
        column['default']=datetime.now().time
      elif colType=='timestamp' and ((not attrDefault and attrNotNull and column['notNull']) or (attrDefault and column['default']=='now')):
        column['default']=datetime.now
      elif colType=='foreignKey' and attrDefault and attrNotNull:
        if column['notNull'] or column['default']!=None:
          column['default']=int(column['default'])
      tmp[colName]=COLUMNS[colType](**column)
    return tmp
  def _jsonJoins(self,joins):
    tmp={}
    for join in joins:
      joinName=join.pop('name')
      joinType=join.pop('type')
      joinClass=join.pop('class')
      tmp[joinName]=JOIN[joinType](joinClass,**join)
    return tmp
  def _jsonIndexes(self,indexes):
    tmp={}
    for index in indexes:
      indexName=str(index.pop('name'))
      indexColumns=[str(x) for x in index.pop('columns')]
      tmp[indexName]=DatabaseIndex(*indexColumns,**index)
    return tmp
  def _jsonProperty(self,schema):
    if 'fromDatabase' in schema:
      prop=self._jsonSqlmeta(schema['tableName'],fromDatabase=schema['fromDatabase'])
    else:
      prop=self._jsonSqlmeta(schema['tableName'])
    if 'columns' in schema:
      prop.update(self._jsonColumns(schema['columns']))
    if 'joins' in schema:
      prop.update(self._jsonJoins(schema['joins']))
    if 'indexes' in schema:
      prop.update(self._jsonIndexes(schema['indexes']))
    return prop
  def resourceFromJSON(self,path=None,url=None,createTable=False,dropTable=False):
    if (not path and not url) or (path and url):
      raise Exception('Invalid path or JSON schema')
    elif path:
      f=open(path,'r')
      raw=f.read()
      f.close()
    elif url:
      raw=urllib2.urlopen(url).read()
    data=json.loads(raw.replace('\r','').replace('\n','').replace('\t','').replace('\'','"'))
    self.schemaClass=[]
    for schema in data:
      prop=self._jsonProperty(schema)
      self.logger.debug(prop)
      self.schemaClass.append(type(str(schema['class']),(Schema,),prop))
    if dropTable:
      for table in self.schemaClass[::-1]:
        table.dropTable(ifExists=True)
    if createTable:
      for table in self.schemaClass:
        table.createTable(ifNotExists=True)
    self.resourceFromSchema(self.schemaClass)
