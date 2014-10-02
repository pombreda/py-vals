#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys,os, time
import time
import json
import datetime
import urlparse
try:
    import regex as re
except ImportError:
    import re
import baker

def die(*args):
    print >>sys.stderr, json.dumps(('error',)+args)
    sys.exit(1)

def ignore_sigpipe(f):
    """decorator to supress sigpipe error"""
    def wrap(*args, **kw):
        try:
            return f(*args, **kw)
        except IOError, e:
            if e.errno!=32:
                raise
    wrap.func_name=f.func_name
    wrap.__name__=f.__name__
    return wrap

def read_lines(fh=sys.stdin, strip=True):
    """unbuffered readlines"""

    if strip:
        proc=lambda l: l.strip('\n')
    else:
        proc=lambda l: l

    while True:
        line=fh.readline()
        if not line:
            break
        yield proc(line)

### duration syntax
def _dclasses():
    s=1
    m=60
    h=60*m
    d=h*24
    w=d*7
    n=d*30
    y=d*365
    return locals()
dclasses=_dclasses()

def duration_notation_to_sec(dn):
    if not isinstance(dn, basestring):
        return dn
    m=re.match('^(\d+)(\p{Alphabetic=Yes})$', dn)
    dnum,dclass=m.groups()
    duration_s=int(dnum)*dclasses[dclass]
    return duration_s
###

@baker.command
def times(duration="24h", step=60*60, direction=-1, start=None):
    """sequence of time in unix epoch"""
    duration_s=duration_notation_to_sec(duration)
    step_s=duration_notation_to_sec(step)
    if not start:
        start=int(time.time())

    for t in range(start, start+direction*duration_s, direction*step_s):
        print t

@baker.command
def slowly(dwell=1):
    """slow down the stream"""

    dwell=float(dwell)

    while True:
        line=sys.stdin.readline()
        if not line:
            break
        sys.stdout.write(line)
        sys.stdout.flush()
        time.sleep(dwell)

@baker.command
def db_zip_rows():
    """heading+rows to json dict stream"""

    val_filter=lambda v: None if v=="NULL" else v

    line=sys.stdin.readline()
    heading=line.strip('\n').split('\t')
    for line in sys.stdin.readlines():
        cols=[ val_filter(c) for c in line.strip('\n').split('\t') ]
        print json.dumps(dict(zip(heading,cols)))

# xx rename to read_jsons()
def vals_from_json():
    while True:
        line=sys.stdin.readline()
        if not line:
            break
        yield json.loads(line)

@baker.command
def dict_trans_key(key_mapping):
    """translate keys in dict stream"""

    key_mapping=json.loads(key_mapping)
    
    for d in vals_from_json():
        print json.dumps(dict([ (key_mapping.get(k,k),v) for k,v in d.items() ]))

@baker.command
def dict_trans_val(val_mapping):
    """translate values in dict stream.

    ### examples
    * normalize datetime
      echo '{"timestamp":"Sun May 11 09:32:33 PDT 2014"}' | dict_trans_val '{"timestamp":"iso8061z"}' 
      {"timestamp": "2014-05-11T09:32:33-07:00Z"}
    """
    # todo: generalize this to arbitrary path and operator
    # A lot of massaging can be done with built-in python expressions.
    # For anything else, functions have to be defined here.
    import dateutil.parser
    ops=dict(
        iso8061=lambda dt: dateutil.parser.parse(dt).isoformat(),
        iso8061z=lambda dt: dateutil.parser.parse(dt).isoformat()+'Z',
             )

    val_mapping=json.loads(val_mapping)
    mapper=dict( [ (k,ops[transformer_name]) for k,transformer_name in val_mapping.items() ])

    d2={}
    for d in vals_from_json():
        for k,v in d.items():
            transformer=mapper.get(k)
            v2=transformer(v) if transformer else v
            d2[k]=v2
        print json.dumps(d2)

@baker.command
def dict_update(update):
    """apply delta to dict streams
    """

    updated=json.loads(update)
    for d in vals_from_json():
        d.update(updated)
        print json.dumps(d)

@baker.command
def dict_prune(keys):
    """filter incoming dicts by keys
    """
    keys=set(keys.split())
    for d in vals_from_json():
        print json.dumps(dict([(k,v) for k,v in d.items() if k not in keys]))

@baker.command
def x_jsonpath(selector):
    """select elements with jsonpath in dict stream.
    """

    from jsonpath import jsonpath
    for val in vals_from_json():
        print jsonpath(val, selector)

# deprecate
def _regex_parse(rx):
    """apply regex to text stream and yield match dicts
    """

    rxo=re.compile(rx)
    
    for line in read_lines():
        m=rxo.search(line)
        if m:
            yield m.groupdict()
# deprecate
@baker.command
def text_parse(rx):
    """regex-based text parsing"""
    for match_dict in _regex_parse(rx):
        print json.dumps(match_dict)

@baker.command
def regex_parse(rx, field='text'):
    """apply regex to text stream and yield match dicts
    """

    rxu=rx.decode('utf8')
    rxo=re.compile(rxu, flags=re.U)
    
    for val in vals_from_json():
        text=val.get(field)
        if text:
            m=rxo.search(text)
            val['match']=m.groupdict() if m else None
        print json.dumps(val)
        sys.stdout.flush()

@baker.command(name='enumerate')
def _enumerate(key='id', **opt):

    for i,v in enumerate(vals_from_json()):
        assert isinstance(v, dict)
        v['id']=i
        print json.dumps(v, **opt)

@baker.command(name='enumerate_l')
def _enumerate(**opt):

    for i,v in enumerate(vals_from_json()):
        assert isinstance(v, dict)
        v.insert(0, i)
        print json.dumps(v, **opt)

######## dbi
# python does not seem to have have standard driver-independent dbi like perl5...

def mysql_db(host, user, password, dbname, port=3306):

    try:
        import MySQLdb
    except ImportError, e:
        die('error', repr(e), 'try apt-get install python-mysqldb')

    db=MySQLdb.connect(host, user, password, dbname, port=port)
    db.set_character_set('utf8')
    
    return db

def postgres_db(host, user, password, database):

    try:
        import psycopg2
    except ImportError, e:
        dir('error', repr(e), 'try apt-get install python-psycopg2')

    return psycopg2.connect(host=host, user=user, password=password, database=database)

db_ctors=dict(mysql=mysql_db,
              postgres=postgres_db)

def unicodify(s):
    return s.decode('utf8') if isinstance(s, str) else s

def stringify(x):
    if isinstance(x, unicode):
        return x.encode('utf8')
    return x

def db_connect(db_url):

    u=urlparse.urlparse(db_url)
    try:
        db_ctor=db_ctors[u.scheme.lower()]
    except KeyError, e:
        die("unsupported db driver", u.scheme, db_url)
    opt={}
    if u.port:
        opt['port']=u.port
    db=db_ctor(u.hostname, u.username, u.password, os.path.basename(u.path), **opt)

    return db

def _db_rows(db, sql, vals=None):
    """mysql://{user}:{pass}@{dbhost}/{schema}"""

    # dwimmy arg processing.
    if sql.startswith('.') or sql.startswith('/'):
        sql=file(sql).read()

    if vals is None:
        vals=()

    cur = db.cursor()
    try:
        # python's db drivers don't have prepared statements like perl's DBI, 
        # so we are still composing sql by string interpolation, making us vulnerable to injection..
        cur.execute(sql, vals)
    except Exception, e:
        e.args+=(sql, vals)
        raise
    if cur.description:
        cols=[d[0] for d in cur.description]
        for row in cur.fetchall():
            unicode_row=map(unicodify,row)
            yield dict(zip(cols,unicode_row))
    cur.close()

import decimal
class ExtraEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime.datetime,datetime.date)):
                #return time.mktime(obj.timetuple())
                return obj.isoformat()
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            return json.JSONEncoder.default(self, obj)

@baker.command
def db_rows(db_url, sql):
    """dump clips from db
    """
    db=db_connect(db_url)
    for row in _db_rows(db, sql):
        print json.dumps(row, cls=ExtraEncoder)
        sys.stdout.flush()

@baker.command
def db_do(db_url, sql, commit_every=100):
    """execute sql for each tuple read from stdin as json list.
* examples:
        echo '[ 0, "gold_coin" ]' | db_do postgres://alibaba:sesami@cave/treasure "update stash count = %s where item = %s;"
    """

    db=db_connect(db_url)
    cur = db.cursor()
    # 
    # todo: commit on finally clause. specifically commit on io (std) error.
    # 
    for i,vals in enumerate(vals_from_json()):

        str_vals=map(stringify,vals)
        try:
            r=cur.execute(sql, str_vals)
        except Exception, e:
            # todo: option to control how it's handled.
            print json.dumps(dict(error=repr(e), sql=sql, vals=vasl))
            raise

        if (i%commit_every)==0:
            db.commit()

	print json.dumps(dict(rowcount=cur.rowcount, lastrowid=cur.lastrowid, vals=vals))
	sys.stdout.flush()

    db.commit()
    cur.close()

@baker.command
def db_insert(db_url, table, commit_every=100):
    # todo: take direct value dict: { created: 'now()' } would add 'now()' to the vals.

    db=db_connect(db_url)
    cur = db.cursor()

    sqlt='INSERT INTO {table} ({cols}) VALUES ({placeholders});'

    for i,rowd in enumerate(vals_from_json()):

        rowd2=dict( [ (k,v) for k,v in rowd.items() if v is not None ] )
        items=rowd2.items()
        placeholders=','.join([ '%s' ] * len(items))
        vals=[v for k,v in items]
        sql=sqlt.format(table=table,
                        cols=','.join([ k for k,v in items ]),
                        placeholders=placeholders)
        r=cur.execute(sql, vals)
        # print r
        if (i%commit_every)==0:
            db.commit()

    db.commit()
    cur.close()

    

@baker.command
def csv_to_json():

    # xx use csv lib..
    for line in sys.stdin.readlines():
        print json.dumps(line.strip('\n').split('\t'))

@baker.command
def update_with_map(mapping, lookup, update):

    # load mapping.
    mappingd=dict([ l.decode('utf8').strip('\n').split('\t') for l in file(mapping).readlines() ])
    
    for v in vals_from_json():

        v[update]=mappingd[v[lookup]]
        print json.dumps(v)

@baker.command
def update(**vals):
    """static update of dict stream"""

    for v in vals_from_json():

        v.update(vals)
        print json.dumps(v)

#### url
import urllib
import urllib2

@baker.command
def url_compose(url, query_dict_key='query', url_key='url'):
    """compose url
    base_url: { .. query: { .. } .. } --> { .. query: { .. }, url: .. }
    """

    for val in vals_from_json():
        query_dict=val[query_dict_key]
        query_string=urllib.urlencode( dict( (k, v.encode('utf8') if isinstance(v,unicode) else v) for k,v in query_dict.items()) )
        val[url_key]='?'.join([url,query_string])
        print json.dumps(val)
        sys.stdout.flush()

@baker.command
def dvl_zip(key1, key2, outkey=None, pop=False):
    """
    [['john', 'mike'], ['smith', 'portnoy']] --> [['john', 'smith'], ['mike', 'portnoy']]
    """
    keys=[key1, key2]
    if not outkey:
        outkey='_'.join(keys)

    for line in sys.stdin.readlines():
        dct=json.loads(line)
        dct[outkey]=zip(*[dct[k] for k in keys])
        if pop:
            for k in keys:
                del dct[k]
        print json.dumps(dct)

@baker.command
def url_escape():

    for line in sys.stdin.readlines():
        print urllib.quote(line.strip('\n'))

@baker.command
def url_unescape():

    for line in sys.stdin.readlines():
        print urllib2.unquote(line.strip('\n'))

if __name__=='__main__':

    try:
        baker.run()
    except IOError, e:
        if e.errno!=32:
            raise
