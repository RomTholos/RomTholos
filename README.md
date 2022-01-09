# RomTholos
Proof of Concept ROM manager

Basic Proof of concept code will follow in the coming days after some initial cleanup's.

## Concept
The ROM manager consist of various utilities that can be used together or independet. Unlike other tools, the main focus is to allow easy integration of various optimized external compression and hashing tools by the end user. It also allows to abstract the storage format by introducing standardised sidecar files and still offer direct comparison to standard DAT files.

## The RSCF sidecar file
The RSCF file includes all relevant information to allow utilities to directly compare an archive to a DAT without the need for de-compression. It essentially includes all ROM information also typically found in a DAT file for a single game. The main information is stored in a standard BSON[^bsonspec] data structure specified by the MongoDB project, wrapped in a propriatary envelop, including a hash of the data structure for data integrity verification.

The typical use case is to compress a certain RAW file format like <.iso> or <.bin/.cue> and compress them with 7zip (7z), chdman (CHD), dolphin (RVZ) or other tools to spare some space and still allow the files to be consumed by emulators. The main problem is, that upstream DAT files store and list the file metadata only for the orignal RAW file format. This approach is correct, since folks will develop new advanced file formats to spare space in the future. Most of these fileformats do unfortunatly not store the hashes of the original RAW files for comparison at a later date against an upstream DAT. Therefore we first calculate the hash of the original RAW file, compress the files with a profile based 'renderer' and then create a sidecar file for verification without the presence of the original RAW files. The tool only will and shall support 'loosless' fileformats, who allow a back-conversion to RAW.

### File Spec
The Header, BSON Payload and Footer are all written in binary mode to a file. The resulting file is typically between 500 bytes to 35 kbytes, depending on the file count.

#### Header
```python
'\x01' + <bson_payload_sha-256> + '\x1e\x02\x02\x02'
```
#### BSON Payload
```python
{
    'version': 0,               # File format version
    'file_blake3': 0,           # Blake3 hash of container
    'file_mtime': 0,            # Modification time of container
    'file_size': 0,             # File size of container
    'files': {
      <romIndex>: {                 # Index starts always with 0      # Used by:
        'path':   <romfilepath>,    # Relative path inside archive    # DAT, archive.org, SMDB
        'size':   <romfilesize>,    # File size                       # DAT, archive.org
        'mtime':  <romfilemtime>,   # Modification date               # archive.org, DOS games
        'crc32':  <romfilecrc32>,   # CRC32 hash                      # DAT, archive.org 
        'md5':    <romfilemd5>,     # MD5 hash                        # DAT, archive.org
        'sha1':   <romfilesha1>,    # SHA-1 hash                      # DAT, archive.org
        'sha256': <romfilesha256>,  # SHA-256 hash                    # SMDB compatibility
        'blake3': <romfileblake3>   # Blake3 hash                     # For speed and future use
      },
      <romIndex+1>: {
        'path':   <romfilepath>, 
        'size':   <romfilesize>,    
        'mtime':  <romfilemtime>,
        'crc32':  <romfilecrc32>,
        'md5':    <romfilemd5>,
        'sha1':   <romfilesha1>,
        'sha256': <romfilesha256>,
        'blake3': <romfileblake3>
      }
    },
    'renderer': 'main.7z-lzma'  # none, main.7z-lzma , main.7z-zstd, ...
}
```
#### Footer
```python
'\x03\x03\x03\x04'
```
### GPG Signing (optional)
RSCF can be signed with GPG for tampering protection. Files shall be using the .sig extension.

### Sample Folder Layout
```
romRoot/
   |
   |--- game_archive_a.7z         # Compressed game files
   |--- game_archive_a.7z.rscf    # File metadata and signatures
   |--- game_archive_a.7z.sig     # Optional GPG signature for rscf tampering protection
   |--- game_archive_b.7z
   |--- game_archive_b.7z.sig
   |--- ...
```

## Renderer
A 'renderer' is basically a profile which stores information how to compress a RAW file to a target archive type. It typically includes information on:
  1. How to pack a single file
  2. How to pack a complete game (One or more files)
  3. How to unpack a single file
  4. How to unpack a complete game (One or more files)
  5. Command line parameters

### Default profiles
#### none
Plain files without compression.

#### main.7z-lzma
7z container with standard LZMA compression.

#### main.7z-zstd
7z container with standard zStandard compression.

#### User defined profiles
The user can alter profiles and configure new tools that may become available in the future. He shall name them with <user.username.profilename> where 'username' and 'profilename' can be choosen by the user. E.g. user.gregor.myprofile

Usefull profiles can be submitted as a merge request for re-use by other users.

## Footnotes
[^bsonspec]: https://bsonspec.org/spec.html

