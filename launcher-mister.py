import subprocess, argparse, json
from pathlib import Path, PurePosixPath
import sys

MISTER_IP = ""
MISTER_USER = ""
MISTER_PASSWORD = ""
USE_PASSWORD = True
TARGET_GAME_FOLDER = PurePosixPath("/media/fat/games/__launcher")

def parse_arguments():
    ### Parsing command line arguments
    parser = argparse.ArgumentParser(
        description = 'RomTholos MiSTer game launcher')

    parser.add_argument('-p',
        required = True,
        type=str,
        help = 'Launch Platform. [genesis, snes]')

    parser.add_argument('-r',
        required = True,
        type=Path,
        help = 'Path to ROM')

    args = parser.parse_args()
    return args

def load_config():
    conf_path = Path("conf/launcher-mister.json")
    with conf_path.open(mode='r') as f:
        data = json.load(f)

    global MISTER_IP
    global MISTER_USER
    global MISTER_PASSWORD
    global USE_PASSWORD

    MISTER_IP = data['mister_ip']
    MISTER_USER = data['mister_user']
    MISTER_PASSWORD = data['mister_password']
    USE_PASSWORD = data['use_password']


def ssh_cmd(cmd):

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

def ssh_scp(src,target):
    # TODO: Add pub/priv key auth
    if True:
        result = subprocess.run(['pscp', '-l', MISTER_USER, '-pw', MISTER_PASSWORD, src, f'{MISTER_IP}:{target}'], 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)

def write_mgl(platform, rom_path):
    # <mistergamedescription>
	#     <rbf>_console/genesis</rbf>
    #     <file delay="2" type="f" index="0" path="../../_mgl/__games/Sonic The Hedgehog 2 (World) (Rev A).md"/>
    # </mistergamedescription>

    if platform == 'genesis':
        rom_path = Path(rom_path)
        game_name = rom_path.stem
        mgl_path = Path("./temp/") / Path(game_name).with_suffix('.mgl')
        
    with mgl_path.open(mode='w') as f:
        f.write('<mistergamedescription>\n')
        f.write('\t<rbf>_console/genesis</rbf>\n')
        f.write(f'\t<file delay="2" type="f" index="0" path="../../_mgl/__games/Genesis/{rom_path.name}"/>\n')
        f.write('</mistergamedescription>')

    ssh_scp(mgl_path, '/media/fat/_mgl/')

    return mgl_path

def copy_rom(platform, rom_path):
    #pscp -l root -pw 1 -r '.\ingest\Sonic the Hedgehog 2 (World) (Rev A).md' 10.23.0.61:/media/fat/_mgl/__games/
    
    if platform == 'genesis':
        mister_path = '/media/fat/_mgl/__games/Genesis/'
        ssh_cmd('mkdir -p /media/fat/_mgl/__games/Genesis/')

    ssh_scp(rom_path, mister_path)

def play_rom(platform, rom_path, mgl):
    #plink -batch -l root -pw 1 10.23.0.61 'echo "load_core /media/fat/_mgl/Sonic the Hedgehog 2.mgl" > /dev/MiSTer_cmd'
    cmd="echo \"load_core /media/fat/_mgl/" + f'{mgl}' + "\" > /dev/MiSTer_cmd"
    print(cmd)

    if platform == 'genesis':
        print('SEGA - GENESIS/MegaDrive')

    ssh_cmd(cmd)

def clean_up(mgl):
    mgl.unlink()

def launch_rom(platform, rom_path):
    mgl = write_mgl(platform, rom_path)
    copy_rom(platform, rom_path)
    play_rom(platform, rom_path, mgl.name)
    clean_up(mgl)

    
def main():
    args = parse_arguments()
    load_config()

    launch_rom(args.p, args.r)

    input("Press enter to exit")

if __name__ == '__main__':
    main()