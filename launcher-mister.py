import subprocess
import ftplib
from pathlib import Path, PurePosixPath
from sys import stdout

MISTER_IP = "192.168.1.10"
MISTER_USER = 'root'
MISTER_PASSWORD = '1'
USE_PASSWORD = True
TARGET_GAME_FOLDER = PurePosixPath("/media/fat/games/__launcher")

def play_rom(src_path):
    #plink -batch -l root -pw 1 10.23.0.61 'echo "load_core /media/fat/_mgl/Sonic the Hedgehog 2.mgl" > /dev/MiSTer_cmd'
    cmd="echo \"load_core /media/fat/_mgl/Sonic the Hedgehog 2.mgl\" > /dev/MiSTer_cmd"

    if USE_PASSWORD == True:
        print('Login with password. Use private/public key auth if possible!')
        result = subprocess.run(['plink', '-batch', '-l', MISTER_USER, '-pw', MISTER_PASSWORD, MISTER_IP , cmd], 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
    else:
        print('Login using private/public key. Load key with pageant.')
        result = subprocess.run(['plink', '-batch', '-l', MISTER_USER, '-pw', MISTER_PASSWORD, MISTER_IP , cmd], 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
    
def main():
    play_rom('./ingest/Sonic the Hedgehog 2 (World) (Rev A).md')

if __name__ == '__main__':
    main()