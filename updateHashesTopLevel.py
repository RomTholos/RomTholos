import glob, os, sys
import hashlib, zlib, hmac
import py7zr
from utils import rscf, b3sum, fs, par2
import argparse, json
import msgpack 
from pathlib import Path

# Parameters
fileRoot = ''
cacheRoot = ''
r_cacheRoot = Path(r'cache/')
cacheRootBig = ''
metaRoot = ''

option_sidecar = True

option_scanmode = 'fast'
option_inode = True

option_hmac_key = 'password'
option_hmac = True
rscf_header_version = '\x00\x00\x00\x01' # Container Version 1

### Parsing command line arguments
parser = argparse.ArgumentParser(
    description = 'RSCF Updater')
parser.add_argument('action',
    choices = ['update', 'verify', 'clean', 'par2'],
    help = 'Select what action to perform.')
parser.add_argument('-r', '--rootdir',
    required = True,
    help = 'File root. Process recursivly from here.')
parser.add_argument('-c', '--cachedir',
    required = False,
    default = "cache/",
    help = 'Scratchpad area. All decompressed files will be written here.')
parser.add_argument('-m', '--metadir',
    required = False,
    default = 'sidecar',
    help = 'Placing RSCF, PAR2 archives and GPG signatures out of three in this directory.')
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

if os.path.isdir(args.cachedir) and args.cachedir != "":
    cacheRoot = args.cachedir
    raw_cacheRoot = r'{}'.format(cacheRoot)
    r_cacheRoot = Path(raw_cacheRoot)
else:
    sys.exit("Specfiy valid directory")
    
if os.path.isdir(args.metadir) and args.metadir != 'sidecar':
    metaRoot = args.metadir
    option_sidecar = False
else:
    metaRoot = fileRoot
    option_sidecar = True

### Data Container
# rscfTemplate = {
    # 'version': 0,
    # 'file_blake3': 0,
    # 'file_mtime': 0,
    # 'file_ctime': 0,
    # 'file_size': 0,
    # 'file_inode': 0,
    # 'files': {},
    # 'renderer': 'main.7z-lzma' # none, main.7z-lzma , main.7z-zstd, ...
# }
    
def getFiles(path):
    fileList = []
    
    # Process every file in root_dir
    for filename in glob.iglob(os.path.join(path, '**/*.*'), recursive=True):
        if os.path.isfile(filename) and filename[-5:] != ".rscf" and filename[-5:] != ".par2" and filename[-4:] != ".sig" :
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
def checkRscfExists(file_tuple, target):
    path = file_tuple[0]
    path = path.with_suffix(f'{path.suffix}.rscf')
    if path.is_file():
        r = True

    else:
        r = False
        
    return r
    
def checkPar2Exists(path):
    if os.path.isfile(path+'.par2'):
        r = True

    else:
        r = False
        
    return r

def compressFile(filePath, fileName, method="py7zr"):
    # Allow various compression methods to be implemented.
    #  Goal 1: Allow external compressors if needed.
    #  Goal 2: Allow multithreaded compression

    # Default: Python py7zr compression
    if method == "py7zr":
        with py7zr.SevenZipFile(fileName + '.7z', 'w') as a:
            a.write(filePath, fileName)

# 2.1 If rscf file is present, verify

# 2.2 If not present generate new rscf file, top level

# TODO Use a cache and update 2.2, including all sub files (ROM's)
# TODO Deep verification level with unpacking archive

# Check if cache is not empty
if any(Path(r_cacheRoot).iterdir()):
    sys.exit("Assigned cache contains files. Abort.")

###########################
## Action: update
########################### 
if args.action == 'clean':
    if os.path.isdir(fileRoot):
        cleanRSCF(fileRoot)

###########################
## Action: par2
########################### 
if args.action == 'par2':
    if os.path.isdir(args.rootdir):
               
        fileList = getFiles(fileRoot)
        for file in fileList:
            e = checkPar2Exists(file)
            
            if e is False:
                par2.create_par2(file)
            
            o = par2.verify_par2(file)
            
            print(f'Par2 verified {str(o)} for file {file}')

###########################
## Action: update
###########################       
if args.action == 'update':
    if os.path.isdir(args.rootdir):
        path = Path(args.rootdir)
        fileList = fs.get_files(path, recursive=True, type_filter='*', type_neg_filter=['.rscf','.par2','.sig'])       
        #fileList = getFiles(fileRoot)
        verified = 0
        broken = 0
        for file_tuple in fileList:
            e = checkRscfExists(file_tuple, None)
            #print("RSCF for file " + file + ' exists: ' + str(e))
            
            #file_tuple[n] 0         1         2       3       4
            #file_tuple = (filepath, filesize, c_time, m_time, inode)
            
            file_path  = file_tuple[0]
            file_size  = file_tuple[1]
            file_ctime = file_tuple[2]
            file_mtime = file_tuple[3]
            file_inode = file_tuple[4]
           
            # Create new RSCF file if not exists
            if e is False:
                return_val = rscf.new_file(file_tuple, target=None, cache=r_cacheRoot)
                    
            # Verify top level if RSCF file exists
            elif e is True:
                verificationMode = option_scanmode
                rscfIntegrity = True
                rscfRewrite = False
                
                t_target = file_path.with_suffix(f'{file_path.suffix}.rscf')
                rscf_r = rscf.read_rscf(t_target)

                if rscf_r == False:
                    return_val = rscf.new_file(file_tuple, target=None, cache=r_cacheRoot)
                    verificationMode = None
                
                print('Processing file: ' + str(file_tuple))
                
                if verificationMode == 'fast':
                    if not rscf_r['file_mtime'] == file_mtime:
                        print('mtime not correct, fallback to hash verification.')
                        verificationMode = 'hash'
                        rscfIntegrity = False
                        rscfRewrite = True
                        
                    if not rscf_r['file_ctime'] == file_ctime:
                        print('ctime not correct, fallback to hash verification.')
                        verificationMode = 'hash'
                        rscfIntegrity = False
                        rscfRewrite = True
                        
                    if option_inode is True:
                        if not rscf_r['file_inode'] == file_inode:
                            print('inode not correct, fallback to hash verification.')
                            print('Warning: Inode verification does not work on all filesystems. Disable if unshure.')
                            verificationMode = 'hash'
                            rscfIntegrity = False
                            rscfRewrite = True
                        
                    if not rscf_r['file_size'] == file_size:
                        print('size not correct, fallback to hash verification.')
                        verificationMode = 'hash'
                        rscfIntegrity = False
                        rscfRewrite = True

                if verificationMode == 'hash':        
                    if not b3sum.get_b3sum(file_path) == rscf_r['file_blake3']:
                        print('Hash does not match!')
                        rscfIntegrity = False
                        rscfRewrite = False
                    else:
                        rscfIntegrity = True
                
                if rscfIntegrity == True:
                        verified += 1
                else:
                    broken += 1
                    print('File does not match: ' + file_tuple)
                    
                if rscfRewrite == True:
                    t_target = file_path.with_suffix(f'{file_path.suffix}.rscf')
                    rscf.update_header(file_tuple, rscf_r, t_target)
                    print('RSCF header rewrittten for file: ' + file_path)
                    
        print('From ' + str(len(fileList)) + ' files,')
        print('\t' + str(verified) + " are OK")
        print('\t' + str(broken) + " are BAD")