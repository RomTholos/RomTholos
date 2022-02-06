import glob, os, sys
import py7zr
from pathlib import Path

# Stat file ctime_ns, atime_ns, size, inode_no (if on regular fs)
# Default ignore inode, but can be configured

option_cache = 'cache/'

# Geth file_tuples for all files in a directory
#file_tuple[n] 0         1         2       3       4
#file_tuple = (filepath, filesize, c_time, m_time, inode)
def get_files(path, recursive=True, type_filter='*', type_neg_filter=[]):

    #Create file list
    file_list = []
    
    if recursive == True:
        type_filter = f'**/{type_filter}'
    
    for p in path.glob(type_filter):
        if p.is_file():
            if not p.suffix in type_neg_filter:
                f_stat = p.stat()
                filesize = f_stat.st_size
                c_time = f_stat.st_ctime_ns
                m_time = f_stat.st_mtime_ns
                inode = f_stat.st_ino
                
                file = p, filesize, c_time, m_time, inode
                file_list.append(file)
            
    return(file_list)
    
    #.glob('*.py') p.glob('*.*.par2')

# Get all files in a folder
def files_from_path(path, ftype='*'):
    
    # Create file lists
    
    fileList = [] #* option_cpu
    
    # Process every file in root_dir
    
    for filename in glob.iglob(path + '**/*.' + ftype, recursive=True):
        if os.path.isfile(filename):
            f_stat = os.stat(filename)
            filesize = f_stat.st_size
            c_time = f_stat.st_ctime_ns
            m_time = f_stat.st_mtime_ns
            inode = f_stat.st_ino
            # Return tuple because of it's low memory footprint
            file = (filename, filesize, c_time, m_time, inode)
            
            fileList.append(file)
    
    return fileList
    
def decompress_7z_all(path, target):
    with py7zr.SevenZipFile(path, 'r') as a:
            a.extractall(target)
            

# Unpacks archives of various types.        
def unpack_archive(path, target=None, type_filter=None):
    #file_tuple[n] 0         1         2       3       4
    #file_tuple = (filepath, filesize, c_time, m_time, inode)
    
    #path = file_tuple[0]
    
    if target is None:
        target = path.parent / path.stem
    
        if not target.is_dir():
            target.mkdir(parents=True)
    
    if path.suffix == '.7z':
        decompress_7z_all(path, target)
    
    #os.remove(in_file)
    print(f'Finished with: {str(path)}')
    
    #time.sleep(1)
    
    return target
    
