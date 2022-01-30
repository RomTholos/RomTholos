import glob, os, sys
import bson
import hashlib, zlib, hmac
import py7zr
from utils import b3sum
import argparse, json

# Parameters
fileRoot = ''
cacheRoot = 'cache/'

option_scanmode = 'fast'
option_inode = False

option_hmac_key = 'password'
option_hmac = True
rscf_header_version = '\x00\x00' # Container Version 0

### Parsing command line arguments
parser = argparse.ArgumentParser(
    description = 'RSCF Updater')
parser.add_argument('action',
    choices = ['update', 'verify', 'clean'],
    help = 'Select what action to perform.')
parser.add_argument('-r', '--rootdir',
    required = True,
    help = 'File root. Process recursivly from here.')
args = parser.parse_args()

# Verify arguments
if args.rootdir == "":
    sys.exit("Please speciffy a working backup set with --set n")
else:
    print(args.rootdir)
    if os.path.isdir(args.rootdir):
        fileRoot = args.rootdir
    else:
        sys.exit("Specfiy valid directory")

### Data Container
rscfTemplate = {
    'version': 0,
    'file_blake3': 0,
    'file_mtime_ns': 0,
    'file_ctime_ns': 0,
    'file_size': 0,
    'file_inode': 0,
    'files': {},
    'renderer': 'main.7z-lzma' # none, main.7z-lzma , main.7z-zstd, ...
}

# 1. Get a list of all files in current directory
def getAllFiles(path):
    fileList = []
    
    # Process every file in root_dir
    for filename in glob.iglob(os.path.join(path, '**/*.*'), recursive=True):
        if os.path.isfile(filename):
            fileList.append(filename)
            print(filename)
    
    return fileList
    
def getFiles(path):
    fileList = []
    
    # Process every file in root_dir
    for filename in glob.iglob(path + '**/*.*', recursive=True):
        if os.path.isfile(filename) and filename[-5:] != ".rscf":
            fileList.append(filename)
    
    return fileList
    
def cleanRSCF(path):
    fileList = []
    
    # Process every file in root_dir
    for filename in glob.iglob(path + '**/*.rscf', recursive=True):
        if os.path.isfile(filename):
            if not os.path.isfile(filename[:-5]):
                print("Original file no longer available. Delete RSCF: " + filename)
                os.remove(filename)

# 2. Check if rscf file is already present
def checkRscfExists(path):
    if os.path.isfile(path+'.rscf'):
        r = True

    else:
        r = False
        
    return r
    
def writeRscf(rscfData, path):
    with open(path+'.rscf', 'wb') as f:
        bson_data = bson.dumps(rscfData)
        bson_digest = hashlib.sha256(bson_data).hexdigest()
        bson_header = '\x01' + rscf_header_version + '\x1d' + bson_digest + '\x1e\x02\x02\x02'
        bson_footer = '\x03\x03\x03\x04'
        f.write(bson_header.encode('ascii'))
        f.write(bson_data)
        f.write(bson_footer.encode('ascii'))

def readRscf(path):
    with open(path, 'rb') as f:
        s = f.read() #unsafe
        sp = s.split(b'\x1e\x02\x02\x02')
        bson_digest_r = sp[0][4:]
        bson_data_r = sp[1][:-4]
        rscf_data = bson.loads(bson_data_r)
    
        bson_digest = hashlib.sha256(bson_data_r).hexdigest()
    
        if str.encode(bson_digest) == bson_digest_r:
            #print("Digest OK")
            #print(rscf_data)
            return rscf_data   

def compressFile(filePath, fileName, method="py7zr"):
    # Allow various compression methods to be implemented.
    #  Goal 1: Allow external compressors if needed.
    #  Goal 2: Allow multithreaded compression

    # Default: Python py7zr compression
    if method == "py7zr":
        with py7zr.SevenZipFile(fileName + '.7z', 'w') as a:
            a.write(filePath, fileName)
            
def decompress7zAll(path, fileName):
    with py7zr.SevenZipFile(path, 'r') as a:
            a.extractall(cacheRoot)
            
def decompressAll(path, fileName):
    if path.endswith('.7z'):
        decompress7zAll(path, fileName)

### Calculate all required file hashes, size and mtime from path
# Modified from https://stackoverflow.com/questions/1742866/compute-crc-of-file-in-python
def getROMMeta(filepath):

    f_size = os.path.getsize(filepath)
    f_mtime = os.path.getmtime(filepath)
    
    h_crc32 = 0
    h_md5 = hashlib.md5()
    h_sha1 = hashlib.sha1()
    h_sha256 = hashlib.sha256()
    h_blake3 = b3sum.getBlake3Sum(filepath)[1]
    
    with open(filepath, 'rb') as fh:
        while True:
            s = fh.read(65536)
            if not s:
                break
            h_crc32 = zlib.crc32(s, h_crc32)
            h_md5.update(s)
            h_sha1.update(s)
            h_sha256.update(s)
        
        h_crc32 = "%08X" % (h_crc32 & 0xFFFFFFFF)
        h_md5 = h_md5.hexdigest().upper()
        h_sha1 = h_sha1.hexdigest().upper()
        h_sha256 = h_sha256.hexdigest().upper()

        return [f_size, f_mtime, h_crc32, h_md5, h_sha1, h_sha256, h_blake3]

# 2.1 If rscf file is present, verify

# 2.2 If not present generate new rscf file, top level

# TODO Use a cache and update 2.2, including all sub files (ROM's)
# TODO Deep verification level with unpacking archive

def rscfUpdateHeader(file, rscf):
    rscf['file_blake3'] = b3sum.getBlake3Sum(file)[1]
    romStat = os.stat(file)
    rscf['file_mtime_ns'] = romStat.st_mtime_ns
    rscf['file_ctime_ns'] = romStat.st_ctime_ns
    rscf['file_inode'] = romStat.st_ino
    rscf['file_size'] = romStat.st_size
    
    writeRscf(rscf,file)

def processFile(file):
    rscf = rscfTemplate
    rscf['file_blake3'] = b3sum.getBlake3Sum(file)[1]
    romStat = os.stat(file)
    rscf['file_mtime_ns'] = romStat.st_mtime_ns
    rscf['file_ctime_ns'] = romStat.st_ctime_ns
    rscf['file_inode'] = romStat.st_ino
    rscf['file_size'] = romStat.st_size
    
    # Decompress ROMs into the cache
    decompressAll(file,"none")
    
    # Get all ROM files
    romList = getFiles(cacheRoot)
    romIndex = 0
    for rom in romList:
        ### Get required file metadata
        fileMeta = getROMMeta(rom)
        
        meta = { 
            romIndex: {
                'path':   os.path.relpath(rom, start=cacheRoot),
                'size':   fileMeta[0],
                'mtime':  fileMeta[1],
                'crc32':  fileMeta[2],
                'md5':    fileMeta[3],
                'sha1':   fileMeta[4],
                'sha256': fileMeta[5],
                'blake3': fileMeta[6]
            }       
        }
        print(meta)
        rscf['files'].update(meta)
        romIndex = romIndex+1
    
    # Write RSCF file
    writeRscf(rscf,file)
    print("RSCF file written")
    
    # Purge cache
    romList = getFiles(cacheRoot) #Update in case something happened
    for rom in romList:
        os.remove(rom)


if args.action == 'clean':
    if os.path.isdir(fileRoot):
        cleanRSCF(fileRoot)
        
if args.action == 'update':
    if os.path.isdir(args.rootdir):
               
        fileList = getFiles(fileRoot)
        verified = 0
        broken = 0
        for file in fileList:
            e = checkRscfExists(file)
            #print("RSCF for file " + file + ' exists: ' + str(e))
            
            # Create new RSCF file
            if e is False:
                processFile(file)
                    
            # Verify top level if RSCF file exists
            elif e is True:
                rscf = readRscf(file+'.rscf')
                
                verificationMode = option_scanmode
                rscfIntegrity = True
                rscfRewrite = False
                
                print('Processing file: ' + file)
                
                if verificationMode == 'fast':
                    romStat = os.stat(file)
                    if not rscf['file_mtime_ns'] == romStat.st_mtime_ns:
                        print('mtime not correct, fallback to hash verification.')
                        verificationMode = 'hash'
                        rscfIntegrity = False
                        rscfRewrite = True
                        
                    if not rscf['file_ctime_ns'] == romStat.st_ctime_ns:
                        print('ctime not correct, fallback to hash verification.')
                        verificationMode = 'hash'
                        rscfIntegrity = False
                        rscfRewrite = True
                        
                    if option_inode is True:
                        if not rscf['file_inode'] == romStat.st_ino:
                            print('inode not correct, fallback to hash verification.')
                            print('Warning: Inode verification does not work on all filesystems. Disable if unshure.')
                            verificationMode = 'hash'
                            rscfIntegrity = False
                            rscfRewrite = True
                        
                    if not rscf['file_size'] == romStat.st_size:
                        print('size not correct, fallback to hash verification.')
                        verificationMode = 'hash'
                        rscfIntegrity = False
                        rscfRewrite = True

                if verificationMode == 'hash':        
                    if not b3sum.getBlake3Sum(file)[1] == rscf['file_blake3']:
                        print('Hash does not match!')
                        rscfIntegrity = False
                        rscfRewrite = False
                    else:
                        rscfIntegrity = True
                
                if rscfIntegrity == True:
                        verified += 1
                else:
                    broken += 1
                    print('File does not match: ' + file)
                    
                if rscfRewrite == True:
                    rscfUpdateHeader(file, rscf)
                    print('RSCF header rewrittten for file: ' + file)
                    
        print('From ' + str(len(fileList)) + ' files,')
        print('\t' + str(verified) + " are OK")
        print('\t' + str(broken) + " are BAD")