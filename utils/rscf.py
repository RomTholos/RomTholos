from pathlib import Path
from utils import b3sum, fs
import os, sys
import msgpack 
import hashlib, zlib, hmac

rscf_header_version = '\x00\x00\x00\x01' # Container Version 1

rscfTemplate = {
    'version': 0,
    'file_blake3': 0,
    'file_mtime': 0,
    'file_ctime': 0,
    'file_size': 0,
    'file_inode': 0,
    'files': {},
    'renderer': 'main.7z-lzma' # none, main.7z-lzma , main.7z-zstd, ...
}

def write_rscf(rscf_data, target):
    
    with target.open(mode='wb') as f:
        mpack_data = msgpack.packb(rscf_data, use_bin_type=True)
        mpack_digest = hashlib.sha256(mpack_data).hexdigest()
        rscf_header = 'RSCF\x01' + rscf_header_version + '\x1d' + mpack_digest + '\x1e\x02\x02\x02'
        rscf_footer = '\x03\x03\x03\x04'
        f.write(rscf_header.encode('ascii'))
        f.write(mpack_data)
        f.write(rscf_footer.encode('ascii'))
        
    return target
        
def read_rscf(path):
    #print(f'Open rscf path: {str(path)}')
    with path.open(mode='rb') as f:
        s = f.read() #unsafe

        sp = s.split(b'\x1e\x02\x02\x02')
        if sp[0][0:5] == b'RSCF\x01' and sp[0][5:9] == str.encode(rscf_header_version):
            mpack_digest_r = sp[0][10:]
            mpack_data_r = sp[1][:-4]
            rscf_data = msgpack.unpackb(mpack_data_r, use_list=False, raw=False, strict_map_key=False)
        
            mpack_digest = hashlib.sha256(mpack_data_r).hexdigest()
        
            if str.encode(mpack_digest) == mpack_digest_r:
                #print("Digest OK")
                #print(rscf_data)
                return rscf_data
            else:
                return False
            
        else:
            return False


### Calculate all required file hashes, size and mtime from path
# Modified from https://stackoverflow.com/questions/1742866/compute-crc-of-file-in-python
# Returns a meta_tuple
#meta_tuple[n] 0       1        2        3        4      5       6         7
#meta_tuple = (f_size, f_ctime, f_mtime, h_crc32, h_md5, h_sha1, h_sha256, h_blake3
def get_file_meta(file_tuple):
    #file_tuple[n] 0         1         2       3       4
    #file_tuple = (filepath, filesize, c_time, m_time, inode)

    path    = file_tuple[0]    
    f_size  = file_tuple[1]
    f_ctime = file_tuple[2]
    f_mtime = file_tuple[3]
    
    h_crc32 = 0
    h_md5 = hashlib.md5()
    h_sha1 = hashlib.sha1()
    h_sha256 = hashlib.sha256()
    h_blake3 = b3sum.get_b3sum(path)
    
    with path.open(mode='rb') as fh:
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

        # meta_tuple
        return (f_size, f_ctime, f_mtime, h_crc32, h_md5, h_sha1, h_sha256, h_blake3)

            
# Accepts an RSCF file tuple and a cache dir  
# cache must be exclusive for this operation          
def new_file(file_tuple, target=None, cache=None):
    #file_tuple[n] 0         1         2       3       4
    #file_tuple = (filepath, filesize, c_time, m_time, inode)
    
    path = file_tuple[0]
    
    if target is None:
        target = path.with_suffix(f'{path.suffix}.rscf')
    
    if target.is_dir():
        target = target / path.with_suffix(f'{path.suffix}.rscf')
        
        if not target.parent.is_dir():
            target.parent.mkdir(parents=True)
            
    rscf = rscfTemplate
    rscf['file_blake3'] = b3sum.get_b3sum(path)
    rscf['file_size']   = file_tuple[1]
    rscf['file_ctime']  = file_tuple[2]
    rscf['file_mtime']  = file_tuple[3]
    rscf['file_inode']  = file_tuple[4]
    
    # Cache must exists
    if cache is None:
        sys.exit("Cache must be specified for this operation.")
    
    # Check if cache is not empty
    if any(Path(cache).iterdir()):
        sys.exit("Assigned cache contains files. Abort.")
    
    # Decompress ROMs into the cache
    fs.unpack_archive(path, cache)
    #decompressAll(str(path),"none")
    
    # Get all ROM files
    romList = fs.get_files(cache)
    romIndex = 0
    for rom in romList:
        ### Get required file metadata
        
        meta_tuple = get_file_meta(rom)
        #meta_tuple[n] 0       1        2        3        4      5       6         7
        #meta_tuple = (f_size, f_ctime, f_mtime, h_crc32, h_md5, h_sha1, h_sha256, h_blake3
        
        meta = { 
            romIndex: {
                'path':   str(rom[0].relative_to(cache)),
                'size':   meta_tuple[0],
                'ctime':  meta_tuple[1],
                'mtime':  meta_tuple[2],
                'crc32':  meta_tuple[3],
                'md5':    meta_tuple[4],
                'sha1':   meta_tuple[5],
                'sha256': meta_tuple[6],
                'blake3': meta_tuple[7]
            }       
        }
        print(meta)
        rscf['files'].update(meta)
        romIndex = romIndex+1
    
    # Write RSCF file
    write_rscf(rscf,target)
    print("RSCF file written")
    
    # Purge cache
    #Update in case something happened
    for path in path.glob(f'{cache}/**/*'):
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            path.rmdir()
        
        
    return target
    
def update_header(file_tuple, rscf_data, target=None):

    path = file_tuple[0]

    rscf_data['file_blake3'] = b3sum.get_b3sum(path)
    rscf_data['file_size']   = file_tuple[1]
    rscf_data['file_ctime']  = file_tuple[2]
    rscf_data['file_mtime']  = file_tuple[3]
    rscf_data['file_inode']  = file_tuple[4]
    
    write_rscf(rscf_data, target)