import sys
import csv
import cStringIO

# Unsure if this matters or not, but using 6.2.0 for now.
splunkVersion = '6.2.0'
headerLen = '0' # empty header

def getTransportHeader(body):
    return 'splunk ' + splunkVersion + ',' + headerLen +  ',' + str(len(body)) + '\n'

def getTransportString(sio):
    body = sio.getvalue()
    return getTransportHeader(body) + body

def makeWriterIO(header):
    sio = cStringIO.StringIO()
    writer = csv.DictWriter(sio, header, extrasaction='ignore')
    writer.writerow(dict(zip(header, header)))
    return writer, sio

# Generates splunk's internal format strings, which can be used with 
# commands.conf: generating = stream
def yieldSplunkStrings(maps, buffersize):
    if len(maps) is 0:
        yield getTransportHeader('')
    else:
        header = list(set(reduce(lambda acc,x: acc + x, map(lambda m: m.keys(), maps), [])))
        writer, sio = makeWriterIO(header)
        hasrows = False
        for m in maps:
            writer.writerow(m)
            hasrows = True
            # flush
            if buffersize < sio.tell():
                yield getTransportString(sio)
                hasrows = False
                writer, sio = makeWriterIO(header)
        if hasrows:
            yield getTransportString(sio)

def write(maps, out=sys.stdout, buffersize=65536):
    try:
        for s in yieldSplunkStrings(maps, buffersize):
            out.write(s)
    finally:
        out.flush()

