import argparse, msgpack 
import sys
from pathlib import Path
import concurrent.futures
import utils.fs as fs
from utils.b3sum import get_b3sum as get_b3sum

# Implements two modes:
#  1. Search for duplicates in path
#  2. Search for duplicates in fork_path against a master_path
#
# The program shall work as a script or as an interactive deduplicator.
# (Ask for user input during operation.)

def parse_arguments():
    ### Parsing command line arguments
    parser = argparse.ArgumentParser(
        description = 'RomTholos file level deduplicator')

    # parser.add_argument('--interactive',
    #     required = False,
    #     action="store_true",
    #     help = 'Operate in interactive mode')

    parser.add_argument('--output',
        required = False,
        type=Path,
        help = 'Write result to file path')

    parser.add_argument('--cachefile',
        required = False,
        type=Path,
        help = 'Cache file stats and hashes to file. Skip expensive full file hashing if already done.')
    
    subparsers = parser.add_subparsers(dest='subcommand',
        help='Select sub command')

    # In place operation in one path
    cupdate_parser = subparsers.add_parser('cache-update', 
        help='Update cache from path')

    cupdate_parser.add_argument('--path',
        required=True,
        type=Path,
        help='Specifify deduplication root path')

    cupdate_parser.add_argument('--cachefile',
        required = True,
        type=Path,
        help = 'Cache file stats and hashes to file. Skip expensive full file hashing if already done.')

    # In place operation in one path
    inplace_parser = subparsers.add_parser('inplace', 
        help='Operate on a single path')

    inplace_parser.add_argument('--path',
        required=True,
        type=Path,
        help='Specifify deduplication root path')

    # Compare operation in one path
    # compare_parser = subparsers.add_parser('compare',
    #     help='Compare a fork path to a protected master path')

    # compare_parser.add_argument('--protected-path',
    #     required=True,
    #     type=Path,
    #     help='Specifify read only master path')

    # compare_parser.add_argument('--fork-path',
    #     required=True,
    #     type=Path,
    #     help='Specifify path for duplicate search')

    args = parser.parse_args()
    return args

def process_file(file_tuple):
    #file_tuple = (filepath, filesize, c_time, m_time, inode)
    path = file_tuple[0]
    if path.exists():
        b3 = get_b3sum(path)
        extended_file_tuple = list(file_tuple)
        extended_file_tuple.append(b3)
        #file_tuple = (filepath, filesize, c_time, m_time, inode, b3sum)
        return extended_file_tuple
    else:
        return None

def output_open(output_path):
    if output_path.is_dir():
        output_path = output_path / "duplicate-list.txt"
        
    if not output_path.exists():
        fh = output_path.open(mode='w')
    else:
        sys.exit("Output file already exists")

    print(f'Write outputs to file: {output_path}')
    return fh

def cachefile_read(cachefile_path):
    if cachefile_path.exists():
        fh = cachefile_path.open(mode='rb')
        mpack_data_r = fh.read() # Unsafe
        cache = msgpack.unpackb(mpack_data_r, use_list=False, raw=False, strict_map_key=False)
        fh.close()
        return cache
    else:
        return None

def cachefile_write(cachefile_path, cache):
    fh = cachefile_path.open(mode='wb')
    fh.write(msgpack.packb(cache, use_bin_type=True))
    fh.close()

def cachefile_open(cachefile_path):
    if cachefile_path.is_dir():
        sys.exit("Cachefile can not be a directory!")

    print(f'Using cachefile: {cachefile_path}')

    cache = cachefile_read()

    fh = cachefile_path.open(mode='wb')

    return fh, cache

def print_output(text_line,fh):
    print(text_line)
    if fh is not None:
        fh.write(text_line)
        fh.write("\n")

def hash_files(file_tuple_list, cache={'files':{}}):
        # Process all other and update cache if requested
        file_list_len = len(file_tuple_list)
        processed = 0

        print('')

        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        
            results = executor.map(process_file, file_tuple_list)
            
            for result in results:
                processed += 1
                print(f'(HASH) Progress: {processed}/{file_list_len}', end='\r')

                if result is not None:
                    # index                  0         1         2       3       4      5
                    # extended_file_tuple = (filepath, filesize, c_time, m_time, inode, b3sum)
                    path = result[0]
                    b3 = result[5]

                    cache['files'][str(path)] = result[1:]

        return cache

def action_cache_update(cache_path, file_list):
    cache = {'files':{}}

    file_list_len = len(file_list)
    processed = 0

    t = cachefile_read(cache_path)
    if t is not None:
        cache = t

    # Check if file is already in cache ([:] Make a copy for iter)
    for file in file_list[:]:

        processed += 1
        print(f'(CACHE-Update) Progress: {processed}/{file_list_len}', end='\r')

        #file_tuple = (filepath, filesize, c_time, m_time, inode)
        if str(file[0]) in cache['files'].keys():
            file_list.remove(file)

    if len(file_list) >= 1:
        cache = hash_files(file_list, cache)
    cachefile_write(cache_path, cache)
    
    print('')

def action_inplace(file_path, file_list, cache_path=None, output=None):
    cache = {'files':{}}
    file_masters = set()        # Set of all selected master files
    dedup_master_cache = {}     # Cache for all unique files under path
    dedup_duplicate_cache = {}  # All selected duplicates
 
    # Open output file if requested
    output_fh = None
    if output is not None:
        output_fh = output_open(output)

    # Read cached hashes from file
    if cache_path is not None:
        action_cache_update(cache_path, file_list.copy())
        cache = cachefile_read(cache_path)
        #print("Successfully updated cache.")

    # Calculate duplicates
    file_list_hash = file_list.copy()
    file_list_len = len(file_list)
    processed = 0

    # Check if file is already in cache
    for file in file_list:
        path = file[0]

        if str(path) in cache['files'].keys():
            file_list_hash.remove(file)

    # Hash all other
    if len(file_list_hash) >= 1:
        cache = hash_files(file_list_hash, cache)

    for file in file_list:
        processed += 1
        print(f'(CACHE-Process) Progress: {processed}/{file_list_len}', end='\r')

        #file_tuple = (filepath, filesize, c_time, m_time, inode)
        path = file[0]

        if str(path) in cache['files'].keys():
            
            b3 = cache['files'][str(path)][4]

            # Detect already seen file
            if b3 in dedup_master_cache.keys():
                # Path length is bigger
                if len(str(path)) > len(str(dedup_master_cache[b3])):
                    dedup_duplicate_cache[path] = b3
                else:
                    dedup_duplicate_cache[dedup_master_cache[b3]] = b3
                    dedup_master_cache[b3] = path

            else:
                dedup_master_cache[b3] = path
    print('')

    for file in dedup_duplicate_cache.values():
            file_masters.add(file)

    print('File duplication list:')
    for file in file_masters:
        print_output('------------------------------------------------------------------', output_fh)
        print_output(f'Master file: {dedup_master_cache[file]}', output_fh)
        for dup_path, dup_b3 in dedup_duplicate_cache.items():
            if dup_b3 == file:
                print_output(f'  * Duplicate: {dup_path}', output_fh)

    # Close output file
    if output_fh is not None:
        output_fh.close()
    

def main():
    args = parse_arguments()


    if args.subcommand == 'cache-update':
        print(f'Get file list from path \'{str(args.path)}\'')
        file_list = fs.get_files(args.path)

        action_cache_update(args.cachefile, file_list)

    elif args.subcommand == 'inplace':
        print(f'Get file list from path \'{str(args.path)}\'')
        file_list = fs.get_files(args.path)

        action_inplace(args.path, file_list, cache_path=args.cachefile, output=args.output)

if __name__ == '__main__':
    main()