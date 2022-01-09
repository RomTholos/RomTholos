import glob, os, sys
import bson
import hashlib, zlib 
import py7zr
from utils import b3sum

# Parameters
fileRoot = 'S:/RomVault/Redump/Sega - Mega CD & Sega CD/'
cacheRoot = 'cache/'

### Data Container
rscfTemplate = {
    'version': 0,
    'file_blake3': 0,
    'file_mtime': 0,
    'file_size': 0,
    'files': {},
    'renderer': 'main.7z-lzma' # none, main.7z-lzma , main.7z-zstd, ...
}

# 1. Get a list of all files in current directory
def getAllFiles(path):
    fileList = []
    
    # Process every file in root_dir
    for filename in glob.iglob(path + '**/*.*', recursive=True):
        if os.path.isfile(filename):
            fileList.append(filename)
    
    return fileList
    
def getFiles(path):
    fileList = []
    
    # Process every file in root_dir
    for filename in glob.iglob(path + '**/*.*', recursive=True):
        if os.path.isfile(filename) and filename[-5:] != ".rscf":
            fileList.append(filename)
    
    return fileList

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
        bson_header = '\x01' + bson_digest + '\x1e\x02\x02\x02'
        bson_footer = '\x03\x03\x03\x04'
        f.write(bson_header.encode('ascii'))
        f.write(bson_data)
        f.write(bson_footer.encode('ascii'))

def readRscf(path):
    with open(path, 'rb') as f:
        s = f.read() #unsafe
        sp = s.split(b'\x1e\x02\x02\x02')
        bson_digest_r = sp[0][1:]
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

fileList = getFiles(fileRoot)
verified = 0
broken = 0
for file in fileList:
    e = checkRscfExists(file)
    #print("RSCF for file " + file + ' exists: ' + str(e))
    
    # Create new RSCF file
    if e is False:
        rscf = rscfTemplate
        rscf['file_blake3'] = b3sum.getBlake3Sum(file)[1]
        rscf['file_mtime'] = os.path.getmtime(file)
        rscf['file_size'] = os.path.getsize(file)
        
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
            
    # Verify top level if RSCF file exists
    elif e is True:
        rscf = readRscf(file+'.rscf')

        if b3sum.getBlake3Sum(file)[1] == rscf['file_blake3']:
            verified = verified + 1
        else:
            broken = broken + 1
            print('File does not match: ' + file)
            
print('From ' + str(len(fileList)) + ' files,')
print('\t' + str(verified) + " are OK")
print('\t' + str(broken) + " are BAD")