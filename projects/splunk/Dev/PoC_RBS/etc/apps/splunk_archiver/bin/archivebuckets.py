#
# Gets everything for executing hunkroll
#

import os, sys, logging as logger
import splunk.Intersplunk as isp
import splunk.entity as en
import splunk.search
import splunk.rest as rest
from collections import defaultdict, OrderedDict
import rollerutils
import json
import splunkio
import time
import itertools

APP_NAME = 'splunk_archiver'
INDEXER_SEARCH_COMMAND_NAME = 'copybuckets'

class ErrorSet:
    def __init__(self, capacity):
        self.capacity = capacity
        self.errs = OrderedDict()
    
    def __setitem__(self, key, value):
        ''' Store items in the order the keys were last added 
        and remove the least recently used item if we're over capacity
        '''
        if key in self.errs:
            del self.errs[key]
        if len(self.errs) >= self.capacity:
            self.errs.popitem(last=False)
        self.errs[key] = value

    def __contains__(self, item):
        ''' check if item is in error set and update error set'''
        exists = item in self.errs
        self[item] = None
        return exists

ERRORS_POSTED = ErrorSet(1000)
ERRMSGS_ENABLED = True

def listProviders(ses):
    return en.getEntities('/data/vix-providers', sessionKey=ses, count=0)

def listVixes(ses):
    return en.getEntities('/data/vix-indexes', sessionKey=ses, count=0, search='disabled=0 AND vix.output.buckets.from.indexes=*')

def filterRollProviders(vixes, providers):
    rollProviders = {}
    for k,indexMap in vixes.iteritems():
        p = indexMap['provider']
        if p not in rollProviders and p in providers:
            rollProviders[p] = providers[p]
    return rollProviders

def genSearchString(vixes, providers):
    m = {'vixes' : vixes, 'providers' : providers}
    # Uses nested json.dumps to escape the returned json string from the inner json.dumps
    return '| ' + INDEXER_SEARCH_COMMAND_NAME + ' json=' + json.dumps(json.dumps(m))

def executeSearch(search, **kwargs):
    return splunk.search.searchAll(search, **kwargs)

def prepareSearchExecution():
    rollerutils.copyJars()

def stripVixPrefix(vix):
    ret = {}
    for name, kvs in vix.iteritems():
        ret[name] = {} 
        for k, v in kvs.iteritems():
            if k.startswith('vix.'):
                ret[name][k.replace('vix.', '', 1)] = v
            else:
                ret[name][k] = v
    return ret

def putNamesInVixMap(vix):
    for name, kvs in vix.iteritems():
        kvs['name'] = name
    return vix

def processVixes(entity):
    d = rollerutils.entityToDict(entity)
    strippedVixes = stripVixPrefix(d)
    return putNamesInVixMap(strippedVixes) 

def resToDict(res):
    ret = {}
    for k in res.keys():
        ret[k] = res[k]
    return ret

def writeResults(results, sessionKey):
    dicts =  map(lambda x: resToDict(x), results)
    if len(dicts) is not 0:
        splunkio.write(dicts)
        postErrors(sessionKey, dicts)

def resultHasError(result):
    if ERRMSGS_ENABLED and 'prefix' in result.keys():
        prefix = str(result['prefix'])
        return prefix.startswith('Error') and isNewError(result['_raw'])
    return False

def isNewError(raw):
    return raw not in ERRORS_POSTED

def postErrors(sessionKey, dicts):
    try:
        errors = itertools.ifilter(resultHasError, dicts)
        for err in errors:
            args = { "name": "rollercontroller_"+str(time.time()),
                     "severity": "error",
                     "value": str(err['_time'])+" "+str(err['_raw']) }
            rest.simpleRequest('messages', sessionKey, postargs=args,
                               method='POST', raiseAllErrors=True);
    except Exception, e:
        import traceback
        splunkio.write([{"stack":traceback.format_exc(),"exception":str(e)}])
        
def streamSearch(search, sessionKey):
    count = 0
    it = search.__iter__()
    while not search.isDone:
        while count is search.count and not search.isDone:
            time.sleep(0.1)
        take = search.count - count
        taken = list(itertools.islice(it, 0, take))
        count += len(taken)
        writeResults(taken, sessionKey)
    writeResults(list(it), sessionKey)

def cancelSearch(search):
    try:
        search.finalize()
        search.cancel()
    except:
        pass

def execute():
    try:
        keywords, argvals = isp.getKeywordsAndOptions()
        results,dummyresults,settings = isp.getOrganizedResults()
        sessionKey = settings.get('sessionKey')

        if sessionKey == None:
            return rollerutils.generateErrorResults('sessionKey not passed to the search command, something\'s very wrong!')
       
        #check that the command is being executed by the scheduler 
        sid = settings.get('sid')
        if not sid.startswith('scheduler_') and not argvals.get('forcerun', '') == '1':
           return rollerutils.generateErrorResults('rollercontroller is supposed to be ran by the scheduler, add forcerun=1 to force execution')

        # check if error messaging is disabled
        global ERRMSGS_ENABLED
        ERRMSGS_ENABLED = 'disablemsgs' not in keywords

        providers = processVixes(listProviders(sessionKey))
        rollVixes = processVixes(listVixes(sessionKey))
        rollProviders = filterRollProviders(rollVixes, providers)
        searchString = genSearchString(rollVixes, rollProviders)

        kwargs = {}
        for k in ['owner', 'namespace','sessionKey','hostPath']:
            if k in settings:
                kwargs[k] = settings[k]

        if not os.path.exists(rollerutils.getAppBinJars()):
            # first time we're copying jars, force bundle replication
            kwargs['force_bundle_replication'] = 1

        prepareSearchExecution()

        search = splunk.search.dispatch(searchString, **kwargs)
        try:
            streamSearch(search, sessionKey)
        finally:
            cancelSearch(search)

    except Exception, e:
        import traceback
        splunkio.write([{"stack":traceback.format_exc(),"exception":str(e)}])
    finally:
        sys.stdout.flush()

if __name__ == '__main__':
    execute()

