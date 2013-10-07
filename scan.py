# hackery to make sure to use the bson parser without the C extensions
# since those crash on some of the input.  
#    https://jira.mongodb.org/browse/PYTHON-571
#a="/usr/lib/python2.7/site-packages/pymongo-2.6_-py2.7.egg/"
import sys
a="/usr/local/lib/python2.7/dist-packages/pymongo-2.6_-py2.7.egg"
sys.path.insert(0,a)
import pymongo
assert not pymongo.has_c(), "you must compile pymongo with --no-ext"
import bson
import datetime
import collections

import mmap,os,struct,pdb,json,time,sys

schema = json.load(open('schema'))

def run(filename):
    fd=open(filename,"r")
    # we used mmap before, but converted to stdio while chasing down the
    # bson parser bug.  It's probably safe to change it back.  The slowdown
    # wasn't that bad though, so I didn't bother.
    mm = 0# mmap.mmap(fd,0,access=mmap.ACCESS_READ)
    # pdb.set_trace()
    scan(filename,fd,mm)
    fd.close()
    print ('done',dt(),filename)

t0 = time.time()
def dt(): return time.time()-t0

def runall():
    run(sys.argv[1])

def filesize(fd):
    fd.seek(0,2)
    return fd.tell()

def scan(filename,fd,mm):
    global k,k1
    maxm = filesize(fd)
    def int4(loc):
        #c = mm[loc:loc+4]
        fd.seek(loc)
        c=fd.read(4)
        n, = struct.unpack('<i',c)
        return n

    def strn(i,n):
        #return mm[i:i+n]
        fd.seek(i)
        return fd.read(n)

    k = k1 = 0
    Last = collections.namedtuple('Last',['i','n','b','s','coll'])
    last = Last(-1000000,-1000000,'','','none')
    for i in xrange(maxm-100):
        if i%10000000 == 0: print ('hello',dt(),filename,i,k1)
        n = int4(i)
        if n > 10 and n < 100000:
            if i+n >= maxm: break
            k+=1
            s = strn(i,n)
            try:
                b = bson.BSON(s).decode()
            except Exception,e:
                continue

            ms = list(coll for coll,fields in schema.iteritems()
                      if all(f in b for f in fields))
            assert (len(ms) < 2), (b,ms)
            if ms:
                coll = ms[0]
                k1 += 1
                # print (dt(),filename, k1,coll,k,i,n)
                if n > 10000:
                    print ('large',dt(),filename,coll,i,n)
		if i - last.i < last.n:
                    print ('overlap',dt(),filename,k1,coll,k,i,n,
                           last._replace(b='%d bytes'%len(last.b),
                                         s='%d bytes'%len(last.s))
                           )
                    ofname = 'overlap-' + filename
                    dump(ofname, i, coll, b, s)
                    dump(ofname, last.i, last.coll, last.b, last.s)
                dump(filename, i, coll, b, s)
                # assert (i - last_i > n), (i,last_i,n)
                last = Last(i, n, b, s, coll)

    return k

# we use a custom json encoder to dump out some bson types
# that don't map directly to json types, as their string
# equivalents.
class doc_encoder_(json.JSONEncoder):
    def default(self, x):
        if isinstance(x,datetime.datetime) \
                or isinstance(x,bson.objectid.ObjectId):
            return str(x)
        return json.JSONEncoder.default(self,x)

exts = ['json','offsets2']
Fcached = collections.namedtuple('Fcached', exts)
file_cache = {}
def dump(origin,i,collection, b, s):
    filename = "%s-%s"%(origin, collection)
    if filename not in file_cache:
        file_cache[filename] = \
            Fcached(**dict((e,open('%s.%s'%(filename,e),'w'))
                          for e in exts))
    # the indent=3 parameter makes human readable but bulkier output.
    j = json.dumps(b, cls=doc_encoder_) #, indent=3)
    fc = file_cache[filename]
    print >>fc.offsets2, fc.json.tell(), i, len(s), len(j)
    print >>fc.json, j

runall()
