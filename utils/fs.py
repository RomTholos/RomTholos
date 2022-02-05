import glob, os, sys

# Stat file ctime_ns, atime_ns, size, inode_no (if on regular fs)
# Default ignore inode, but can be configured


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