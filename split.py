import sys
import itertools
import logging
log = logging.warning

chunksize = 2**26
gapsize = 100100
bufsize = 2**20

def hsplit(filename):
    f = open(filename)
    for i in itertools.count(1):
        ofilename = filename+'.%02d'%i
        with open(ofilename,'w') as out:
            log("opened %s"% ofilename)
            chunkleft = chunksize
            while True:
                if chunkleft <= 0:
                    break
                buf = f.read(bufsize)
                if len(buf) == 0:
                    return
                out.write(buf)
                chunkleft -= len(buf)
                log("%d: wrote %d bytes, %d left"%(i, len(buf), chunkleft))
            out.close()
            f.seek(-gapsize, 1)

hsplit(sys.argv[1])
