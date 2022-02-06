# RomTholos
Proof of Concept ROM manager

Warning: Ultra early proof of concept! Do not use it! You could loose files!

Basic Proof of concept code will follow in the coming days after some initial cleanup's.

## Concept
The ROM manager consist of various utilities that can be used together or independet. Unlike other tools, the main focus is to allow easy integration of various optimized external compression and hashing tools by the end user. It also allows to abstract the storage format by introducing standardised sidecar files and still offer direct comparison to standard DAT files.

### Other uses outside of ROM management
Some of the concepts have also proved useful for other topics like basic file and photo deduplication and file backup.

## The RSCF sidecar file
The RSCF file includes all relevant information to allow utilities to directly compare an archive to a DAT without the need for de-compression. It essentially includes all ROM information also typically found in a DAT file for a single game. The main information is stored in a standard Msgpack[^msgpackspec] data structures, wrapped in a propriatary envelop, including an SHA-256 hash of the data structure for data integrity verification and optional signature for tampering protection. The RSCF file is stored alongside the orignal file, or if configured, in a separate directory structure.

The typical use case is to compress a certain RAW file format like <.iso> or <.bin/.cue> and compress them with 7zip (7z), chdman (CHD), dolphin (RVZ) or other tools to spare some space and still allow the files to be consumed by emulators. The main problem is, that upstream DAT files store and list the file metadata only for the orignal RAW file format. This approach is correct, since folks will develop new advanced file formats to spare space in the future. Most of these fileformats do unfortunatly not store the hashes of the original RAW files, packed inside the archive. This requires to always uncompress the archive to get all required hashes for file verification.

```
  ##########################                               #########################                      ###############
  #  RAW / Original Files  #  --> [Extract Meta Data] -->  #  RSCF Meta Data File  #  --> [GPG Sign] -->  #  Signature  #
  ##########################                               #########################  <--|                ###############
    |                                                          |   |                     |                     |
    |                                                          |   |                     |-- [RSCF Verify] <---|
    |--> [Compress Data] --------|                             |   |
                                 |                             |   |
                                 |                             |   |
  ##########################  <--|                             |   |
  #  Compressed Archive    #  <-- [Archive Verify] <-----------|   |                   
  ##########################  <-- [Content Description] <----------|
  
```

In a first step, all original files get analysed and all meta data like file hashes, directory structure, creation and modified date is extracted. All these information are stored in a RSCF sidecar file for later access. After this step, all original files are placed inside a compressed archive of the users choice. This can potentially hide some required file metadata, that now must be retriefed from the RSCF sidecar file. For integrity checks, the RSCF file includes the archive file hash and allows to detect modifications and bit flips in the archive file. To prevent a malicious attack on the RSCF file, a pure optional GPG signature can be attached to the RSCF file. For bit flip protection, a single par2 archive can be created and referenced in the RSCF file to allow reconstruction at a later date.

### File Spec
The Header, BSON Payload and Footer are all written in binary mode to a file. The resulting file is typically between 500 bytes to 60 kbytes, depending on the file count.

#### Header
```python
'RSCF\x01' + <bson_payload_sha-256> + '\x1e\x02\x02\x02'
```
#### Msgpack Payload
```python
{
    'version': 0,                # File format version									# mandatory
    'file_blake3': 0,            # Blake3 hash of container  							# mandatory
    'file_mtime': 0,             # Modification time of container [ns]					# mandatory
	'file_ctime': 0,			 # Creation time (Platform dependent [ns]				# optional
    'file_size': 0,              # File size of container  [bytes]						# mandatory
	'file_inode': 0,			 # File inode number or index on supported filesystems	# optional
    'files': {					 # Only needed for archives								# optional
      <fileIndex>: {             # Index starts always with 0      # Used by:
        'path':   <filepath>,    # Relative path inside archive    # DAT, archive.org, SMDB
        'size':   <filesize>,    # File size                       # DAT, archive.org
        'ctime':  <filectime,    # Creation date				   # Some DOS games
        'mtime':  <filemtime>,   # Modification date               # Some DOS games
        'crc32':  <filecrc32>,   # CRC32 hash                      # DAT, archive.org 
        'md5':    <filemd5>,     # MD5 hash                        # DAT, archive.org
        'sha1':   <filesha1>,    # SHA-1 hash                      # DAT, archive.org
        'sha256': <filesha256>,  # SHA-256 hash                    # SMDB compatibility
        'blake3': <fileblake3>   # Blake3 hash                     # For speed and future use
      },
      <fileIndex+1>: {
        'path':   <romfilepath>, 
        'size':   <romfilesize>,
        'ctime':  <filectime,		
        'mtime':  <romfilemtime>,
        'crc32':  <romfilecrc32>,
        'md5':    <romfilemd5>,
        'sha1':   <romfilesha1>,
        'sha256': <romfilesha256>,
        'blake3': <romfileblake3>
      }
    },
    'renderer': 'main.7z-lzma',  # none, main.7z-lzma , main.7z-zstd, ...
	'parity': {					 # optional block
		'type':   <parity_type>, # par2
		'path':   <parfilepath>,
		'sha256': <parfilehash>
	}
}
```
#### Footer
```python
'\x03\x03\x03\x04'
```
### GPG Signing (optional)
RSCF can be signed with GPG for tampering protection. Files shall be using the .sig extension.

### Sample Folder Layout

#### In tree (sidecar files)
```
romRoot/
   |
   |--- game_archive_a.7z         # Compressed game files
   |--- game_archive_a.7z.rscf    # File metadata and signatures
   |--- game_archive_a.7z.sig     # Optional GPG signature for rscf tampering protection
   |--- game_archive_b.7z
   |--- game_archive_b.7z.sig
   |--- game_archive_b.7z.par2    # Optional par2 file
   |--- ...
```

#### Out of tree
```
romRoot/
   |
   |--- game_archive_a.7z         # Compressed game files
   |--- game_archive_b.7z
   |--- ...
metaRoot/
   |--- game_archive_a.7z.rscf    # File metadata and signatures
   |--- game_archive_a.7z.sig     # Optional GPG signature for rscf tampering protection
   |--- game_archive_b.7z.rscf
   |--- game_archive_b.7z.sig
   |--- game_archive_b.7z.par2    # Optional par2 file
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

## Other notes
First serialisation format was based on BSON [^bsonspec]. Unfortunatly, the de-serialisation of the data was to slow. Switched to msgpack for good.

## Footnotes
[^bsonspec]: https://bsonspec.org/spec.html
[^msgpackspec]: https://github.com/msgpack/msgpack/blob/master/spec.md

