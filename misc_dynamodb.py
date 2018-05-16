
import boto3
from botocore.exceptions import ClientError

import copy

from itertools import *
from functools import *


def aws_to_py(x):
    r'''
    converts from aws's internal json representation
    (e.g., { 'M': { 'xyz': { 'N': '42' }, 'abc': { 'S': 'towel' } } })
    to an equiv (for my purposes, anyway) python value. 
    
    CAVEAT i don't cover every case, and this hasn't been 
    rigorously tested, or even tested (just used a few
    times).
    '''
    kk = list(x.keys())
    # aws typecode
    tc = 'M'
    try: [tc] = list(x.keys())
    except ValueError:
        x = { 'M': x }
    
    if tc == 'N': return '.' in x['N'] and float(x['N']) or int(x['N'])
    if tc == 'S': return x['S']
    if tc == 'BOOL': return x['BOOL'].strip().upper() == 'TRUE' and True or False
    if tc == 'NULL': return None
    
    if tc == 'L': return [ aws_to_py(elem) for elem in x['L'] ]
    
    if tc == 'M':
        dic = { }
        for k,v in x['M'].items():
            dic[k] = aws_to_py(v)
        return dic

    raise Exception('unknown aws type code!!!')
    
            



def db_pager(callopts, callname='scan', awsopts={}):
    r'''
    boto's paginator has some internal retry logic (i think),
    but it can throw ClientErrors (e.g., throttling).
    
    so, for everything that would otherwise interrupt your
    paginator, here's something that remembers where
    you were and retries with exp backoff.
    
    '''
    sesh = boto3.session.Session(**awsopts)
    db = sesh.client('dynamodb')
    
    pager = db.get_paginator(callname)
    opts = copy.copy(callopts)
    
    backoff = 2
    while True:
        try:
            pages = pager.paginate(**opts)
            for pg in pages:
                yield from [ aws_to_py(it) for it in pg['Items'] ]
                
                # in case we need to restart...
                try: opts['ExclusiveStartKey'] = pg['LastEvaluatedKey']
                except KeyError: pass  # last page!
            break
            
        except ClientError as err:
            print("got ClientError {}.\nbacking off for {} sec...".format(err, backoff))
            time.sleep(backoff)
            backoff *= 2
            
    
    
    
    
    