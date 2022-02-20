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
    b3 = get_b3sum(path)
    extended_file_tuple = list(file_tuple)
    extended_file_tuple.append(b3)
    #file_tuple = (filepath, filesize, c_time, m_time, inode, b3sum)
    return extended_file_tuple

def open_output(output_path):
    if output_path.is_dir():
        output_path = output_path / "duplicate-list.txt"
        
    if not output_path.exists():
        fh = output_path.open(mode='w')
    else:
        sys.exit("Output file already exists")

    print(f'Write outputs to file: {output_path}')
    return fh

def open_cachefile(cachefile_path):
    if cachefile_path.is_dir():
        sys.exit("Cachefile can not be a directory!")

    print(f'Using cachefile: {cachefile_path}')
 
    file_cache = None
    if cachefile_path.exists():
        fh = cachefile_path.open(mode='rb')
        mpack_data_r = fh.read() # Unsafe
        file_cache = msgpack.unpackb(mpack_data_r, use_list=False, raw=False, strict_map_key=False)
        fh.close()

    fh = cachefile_path.open(mode='wb')

    return fh, file_cache

def print_output(text_line,fh):
    print(text_line)
    if fh is not None:
        fh.write(text_line)
        fh.write("\n")
        

def main():
    args = parse_arguments()

    file_cache = {
        "files": {

        }
    }    
    file_master_cache = {}
    file_masters = set()
    file_duplicates = {}

    # Open output file if requested
    output_fh = None
    if args.output is not None:
        output_fh = open_output(args.output)

    # Open cache file if requested
    cachefile_fh = None
    if args.cachefile is not None:
        cachefile_fh, content = open_cachefile(args.cachefile)

        if content is not None:
            file_cache = content

    if args.subcommand == 'inplace':
        
        print(f'Get file list from path \'{str(args.path)}\'')
        file_list = fs.get_files(args.path)
        file_list_len = len(file_list)
        processed = 0

        # Check if file is already in cache ([:] Make a copy for iter)
        for file in file_list[:]:

            #file_tuple = (filepath, filesize, c_time, m_time, inode)
            if str(file[0]) in file_cache['files'].keys():
                processed += 1
                print(f'(CACHE) Progress: {processed}/{file_list_len}', end='\r')
                
                # Update master_cache
                path = file[0]
                b3 = file_cache['files'][str(file[0])][4]
                if b3 in file_master_cache.keys():
                    # Path length is bigger
                    if len(str(path)) > len(str(file_master_cache[b3])):
                        file_duplicates[path] = b3
                    else:
                        file_duplicates[file_master_cache[b3]] = b3
                        file_master_cache[b3] = path
                else:
                    file_master_cache[b3] = path

                file_list.remove(file)

        # Process all other and update cache if requested
        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        
            results = executor.map(process_file, file_list)
            
            for result in results:
                processed += 1
                print(f'(HASH) Progress: {processed}/{file_list_len}', end='\r')

                # index                  0         1         2       3       4      5
                # extended_file_tuple = (filepath, filesize, c_time, m_time, inode, b3sum)
                path = result[0]
                b3 = result[5]

                file_cache['files'][str(path)] = result[1:]

                # Detect already seen file
                if b3 in file_master_cache.keys():
                    # Path length is bigger
                    if len(str(path)) > len(str(file_master_cache[b3])):
                        file_duplicates[path] = b3
                    else:
                        file_duplicates[file_master_cache[b3]] = b3
                        file_master_cache[b3] = path

                else:
                    file_master_cache[b3] = path
            
            print(f'Hash comparison complete. Processed: {processed}/{file_list_len}')

    for file in file_duplicates.values():
        file_masters.add(file)

    print('File duplication list:')
    for file in file_masters:
        print_output('------------------------------------------------------------------', output_fh)
        print_output(f'Master file: {file_master_cache[file]}', output_fh)
        for dup_path, dup_b3 in file_duplicates.items():
            if dup_b3 == file:
                print_output(f'  * Duplicate: {dup_path}', output_fh)

    # Close output file
    if output_fh is not None:
        output_fh.close()

    # Write out file cache and close cachefile file
    if cachefile_fh is not None:
        cachefile_fh.write(msgpack.packb(file_cache, use_bin_type=True))
        cachefile_fh.close()

if __name__ == '__main__':
    main()