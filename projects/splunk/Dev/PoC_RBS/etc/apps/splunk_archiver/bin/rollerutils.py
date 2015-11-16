import splunk
import os
from distutils import dir_util
import splunkio
import time

def entityToDict(entity):
    m = {}
    for k, v in entity.iteritems():
        if isinstance(v, splunk.entity.Entity):
            m[k] = entityToDict(v)
        elif isinstance(v, dict):
            m[k] = entityToDict(v)
        else: 
            m[k] = v
    m.pop('eai:acl', None)
    return m


def trimDirToTemplate(template, target):
    """
    Recursively removes any files from target that are not also extant in template. Does do much error checking, as it
    assumes that template files have already been written into target.
    """
    for f in os.listdir(target):
        templateChild = os.path.join(template, f)
        targetChild = os.path.join(target, f)
        if (os.path.isfile(targetChild) and (not os.path.exists(templateChild))):
            os.remove(targetChild)
        elif (os.path.isdir(targetChild)):
            if (os.path.exists(templateChild)):
                trimDirToTemplate(templateChild, targetChild)
            else:
                os.removedirs(targetChild)

def _copyJars(splunkhome, appbinjars):
    splunkjars = os.path.join(splunkhome, 'bin', 'jars') 
    dir_util.copy_tree(splunkjars, appbinjars, update=1, verbose=0)
    trimDirToTemplate(splunkjars, appbinjars)

def getAppBinJars():
    return os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'java-bin', 'jars'))

def copyJars():
    _copyJars(os.environ['SPLUNK_HOME'], getAppBinJars())

# Using this function instead of splunk.Intersplunk.generateErrorResults
# since Intersplunk's function will output csv to stdout and we need to output
# splunkio format, because we're running 'generate = stream' search commands.
def generateErrorResults(msg):
    splunkio.write([{'ERROR':msg, '_raw':'ERROR ' + msg}])




