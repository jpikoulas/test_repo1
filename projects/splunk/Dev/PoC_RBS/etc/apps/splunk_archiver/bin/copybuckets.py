#
# Executes bucket copying on peers/indexers
#

import os, sys, logging as logger
import time
import splunk.Intersplunk
import splunk.entity as en
import splunk.clilib.cli_common as cli_common
from collections import defaultdict
import collections
import subprocess
from subprocess import Popen, PIPE
import traceback
from StringIO import StringIO
import json
import rollerutils
from distutils import spawn
from Queue import Queue, Empty
from threading import Thread
import splunkio

messageQueue = Queue()
END_MSG = 'THE END'

def listIndexes(ses):
    return en.getEntities('/data/indexes', sessionKey=ses, count=0, search='disabled=0')

def getServerId(ses):
    serverInfo =  en.getEntities('/server/info/server-info', sessionKey=ses, count=0)['server-info']
    serverName = 'unknown'
    if 'serverName' in serverInfo:
        serverName = serverInfo['serverName']
    if 'guid' in serverInfo:
        return serverInfo['guid'], serverName
    elif 'unknown' != serverName:
        return serverName, serverName
    else:
        raise Exception('Could not get any server id from indexer')
    
# Resolves whole path names if they are environment variables. It's naive.
def resolveEnvInPath(env, path):
    resolvedPath = []
    for part in path.split(os.path.sep):
        if part.startswith('$') and part[1:] in env:
            resolvedPath.append(env[part[1:]])
        else:
            resolvedPath.append(part)
    return os.path.sep.join(resolvedPath)

def getRequiredArgs(serverId, sessionKey):
    args = {}
    args['splunk.server.uuid'] = serverId
    args['splunk.server.uri'] = cli_common.getMgmtUri()
    args['splunk.server.auth.token'] = sessionKey
    return args

def getProviderEnv(providerMap):
    env = {}
    for k,v in providerMap.iteritems():
        if isinstance(k, basestring) and k.startswith('env.'):
            envName = k.strip('env.')
            env[envName] = v
    return env

def getVixCommand(providerMap):
    commands = {}
    for k,v in providerMap.iteritems():
        if k.startswith('command'):
            if k == 'command':
                commands['command.arg.0'] = v
            else:
                commands[k] = v
    
    commandsByArgOrder = collections.OrderedDict(sorted(commands.items(), 
                                                        key=lambda t: int(t[0].split('.')[2])))
    return [v for k, v in commandsByArgOrder.iteritems()]

def splitCommaList(commaSeparatedList):
    return map(lambda x: x.strip(), commaSeparatedList.split(','))

def keepRollingIndexes(indexes, vixes):
    indexesToKeep = set([])
    rollKey = 'output.buckets.from.indexes'
    for vixMap in vixes:
        if rollKey in vixMap and vixMap[rollKey]:
            indexesToKeep.update(set(splitCommaList(vixMap[rollKey]))) 
    return {k : v for k, v in indexes.iteritems() if k in indexesToKeep}

def killQuietly(proc):
    try:
        proc.kill()
    except:
        pass

def parseRaw(raw):
    try:
        return json.loads(raw)
    except:
        return {'_raw':raw}

def outputLine(line, serverName):
    if line != '':
        message(withHost(parseRaw(line), serverName))

# Executes one java process per index. Can be run in parallel.
def executeJavaProcesses(providers, vixes, indexes, serverId, serverName, sessionKey):
    # should be: for providerName in providers.iteritems():
    for providerName, providerMap in providers.iteritems():
        # Create the command string that will be run in the shell
        command = getVixCommand(providerMap)
        commandstr = ' '.join(map(escape, command))
        #message(withHost({'commandstr': commandstr }, serverName))

        # Create json that'll be sent to SplunkMR's stdin
        javaArgs = {}
        javaArgs['action'] = 'roll'
        javaArgs['args'] = {'roll': getRequiredArgs(serverId, sessionKey)}
        providerMap['family'] = 'hadoop'

        providersVixes = [v for k, v in vixes.iteritems() if v['provider'] == providerName]
        providersIndexes = keepRollingIndexes(indexes, providersVixes)
        javaArgs['conf'] = {'indexes' : providersVixes,
                            'provider' : providerMap,
                            'splunk-indexes' : providersIndexes}
        jsonArgs = StringIO()
        json.dump(javaArgs, jsonArgs)
        #message(withHost({'jsonargs': jsonArgs.getvalue()}, serverName))

        # create environment vars by combining current env with vix configuration
        vixEnv = getProviderEnv(providerMap)
        vixEnv['SPLUNK_LOG_INCLUDE_TIMESTAMP'] = '1' # any splunk truthy value will do
        vixEnv['SPLUNK_LOG_DEBUG'] = providerMap.get('splunk.search.debug', '0')
        myEnv = os.environ.copy()
        myEnv.update(vixEnv)
        # Filter None's. Popen will crash for values set to None.
        myEnv = dict((k,v) for k,v in myEnv.iteritems() if v is not None)
        
        # Do execute the java process
        proc = None
        stdout = None
        logfile = None
        try:
            if spawn.find_executable(command[0]) is None:
                raise Exception('Could not find command=' + command[0])

            filename = os.path.join(os.environ['SPLUNK_HOME'], 'var', 'log', 'splunk', 'splunk_archiver.log')
            logfile = open(filename, 'a') 
            proc = executeJavaProcessWithArgs(commandstr,  myEnv, logfile)
            proc.stdin.write(jsonArgs.getvalue())
            while proc.poll() is None:
                outputLine(proc.stdout.readline(), serverName)
            exit = proc.wait()
            stdout, stderr = proc.communicate()
            for line in stdout:
                outputLine(line, serverName)
            #message(withHost({'exit': str(exit)}, serverName))
        except Exception, e:
            outputError(e, traceback.format_exc())
        finally:
            if proc is not None:
                killQuietly(proc)
            if logfile is not None:
                logfile.close()

# Executes the command right in the shell
def executeJavaProcessWithArgs(command, env, logfile):
    return Popen(command, env=env, shell=True, stdin=PIPE, stderr=logfile, stdout=PIPE)

def getProvidersAndVixes():
    # Ghetto parsing since splunk's parsing doesn't care about quote escaping
    # Only works when there's one and only one argument.
    jsonStr = sys.argv[1:][0].split("=",1)[1]
    if jsonStr == None:
        raise Exception("Missing required json blob to copy buckets")
    jzon = json.loads(jsonStr)
    return (jzon['providers'], jzon['vixes'])

def mapValues(fn, m):
    ret = {}
    for k, v in m.iteritems():
        if isinstance(v, dict):
            ret[k] = mapValues(fn, v)
        elif isinstance(v, basestring):
            ret[k] = fn(v)
        elif isinstance(v, list):
            ret[k] = map(fn, v)
        else:
            ret[k] = v
    return ret

def replaceSplunkHomeBinJars(s):
    return s.replace('$SPLUNK_HOME/bin/jars', rollerutils.getAppBinJars())

def replaceAllSplunkHomeBinJars(m):
    return mapValues(replaceSplunkHomeBinJars, m)

def outputError(e, tb):
    message([{'exception':str(e)}, {'traceback':tb}])

def escape(s):
    return json.dumps(s, ensure_ascii=False)

# Add _raw if it's not there
def withRaw(message):
    if '_raw' in message:
        return message
    else:
        raw = ''
        for k,v in message.iteritems():
            raw += escape(k) + '=' + escape(v) + ' '
        message['_raw'] = raw
        return message

def withHost(message, serverName):
    if 'host' not in message:
        message['host'] = serverName
    return message

# takes an array of dicts or a dict
def message(message):
    if message is END_MSG:
        messageQueue.put_nowait(message)
    elif isinstance(message, dict):
        messageQueue.put_nowait(withRaw(message))
    else:
        for m in message:
            messageQueue.put_nowait(withRaw(m))

def getMessages(timeout):
    messages = []
    now = time.time()
    end = now + timeout
    shouldExit = False
    while now < end:
        try:
            message = messageQueue.get(block=True, timeout=max(0, end - now))
            if message is END_MSG:
                shouldExit = True
                break
            else:
                messages.append(message)
        except Empty:
            break
        now = time.time()
    return (messages, shouldExit)

def messageSH():
    while True:
        try:
            timeout = 1
            messages, shouldExit = getMessages(timeout)
            splunkio.write(messages)
            if shouldExit:
                break
        except:
            pass

def execute():
    exception = False
    t = None
    try:
        results,dummyresults,settings = splunk.Intersplunk.getOrganizedResults()

        sessionKey = settings.get("sessionKey")

        if sessionKey == None:
            return rollerutils.generateErrorResults("username/password authorization not given to 'input'.")

        providers, vixes = getProvidersAndVixes()
        providers = replaceAllSplunkHomeBinJars(providers)
        vixes = replaceAllSplunkHomeBinJars(vixes)
        indexes = rollerutils.entityToDict(listIndexes(sessionKey))
        indexes = keepRollingIndexes(indexes, vixes.values())
        serverId, serverName = getServerId(sessionKey)
        
        # Everything seems ok, start message thread
        t = Thread(target=messageSH)
        t.setDaemon(True)
        t.start()
        executeJavaProcesses(providers, vixes, indexes, serverId, serverName, sessionKey)
    except Exception, e: 
        outputError(e, traceback.format_exc())
    except KeyError, e: 
        outputError(e, traceback.format_exc())
    finally:
        message(END_MSG)
        if t is not None:
            t.join(10)
        sys.stdout.flush()

if __name__ == '__main__':
    execute()

