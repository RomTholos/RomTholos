import argparse
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
    # #file_tuple = (filepath, filesize, c_time, m_time, inode)
    path = file_tuple[0]
    b3 = get_b3sum(path)
    return [path, b3]

def open_output(output_path):
    if output_path.is_dir():
        output_path = output_path / "duplicate-list.txt"
        
    if not output_path.exists():
        fh = output_path.open(mode='w')
    else:
        sys.exit("Output file already exists")

    print(f'Write outputs to file: {output_path}')
    return fh

def print_output(text_line,fh):
    print(text_line)
    if fh is not None:
        fh.write(text_line)
        fh.write("\n")
        

def main():
    args = parse_arguments()

    # Open output file if requested
    fh = None
    if args.output is not None:
        fh = open_output(args.output)
        

    file_cache = {}
    file_duplicates = {}
    file_masters = set()

    if args.subcommand == 'inplace':
        
        print(f'Get file list from path \'{str(args.path)}\'')
        file_list = fs.get_files(args.path)
        file_list_len = len(file_list)
        processed = 0

        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        
            results = executor.map(process_file, file_list)
            
            for result in results:
                processed += 1
                print(f'Progress: {processed}/{file_list_len}', end='\r')

                path = result[0]
                b3 = result[1]

                # Detect already seen file
                if b3 in file_cache.keys():
                    # Path length is bigger
                    if len(str(path)) > len(str(file_cache[b3])):
                        file_duplicates[path] = b3
                    else:
                        file_duplicates[file_cache[b3]] = b3
                        file_cache[b3] = path

                else:
                    file_cache[b3] = path
            
            print(f'Hash comparison complete. Processed: {processed}/{file_list_len}')

    for file in file_duplicates.values():
        file_masters.add(file)

    print('File duplication list:')
    for file in file_masters:
        print_output('------------------------------------------------------------------', fh)
        print_output(f'Master file: {file_cache[file]}', fh)
        for dup_path, dup_b3 in file_duplicates.items():
            if dup_b3 == file:
                print_output(f'  * Duplicate: {dup_path}', fh)

    # Close output file
    if fh is not None:
        fh.close()

if __name__ == '__main__':
    main()