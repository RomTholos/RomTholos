import subprocess, argparse, json
from pathlib import Path, PurePosixPath
import sys

MISTER_IP = ""
MISTER_USER = ""
MISTER_PASSWORD = ""
USE_PASSWORD = True
TARGET_GAME_FOLDER = "_LaunchBox"
TARGET_ROM_SUB = "_roms"
FORCE_OVERWRITE = False

def parse_arguments():
    ### Parsing command line arguments
    parser = argparse.ArgumentParser(
        description = 'RomTholos MiSTer game launcher')

    parser.add_argument('-p',
        required = True,
        type=str,
        help = 'Launch Platform. [genesis, nes, snes]')

    parser.add_argument('-r',
        required = True,
        type=Path,
        help = 'Path to ROM')
    
    parser.add_argument('--force-overwrite',
        required = False,
        action="store_true",
        help = 'Force Owerwrite')

    args = parser.parse_args()

    global FORCE_OVERWRITE
    if args.force_overwrite == True:
        FORCE_OVERWRITE = True

    return args

def load_config():
    conf_path = Path("conf/launcher-mister.json")
    if conf_path.exists() and conf_path.is_file():
        with conf_path.open(mode='r') as f:
            data = json.load(f)

        global MISTER_IP
        global MISTER_USER
        global MISTER_PASSWORD
        global USE_PASSWORD
        global TARGET_GAME_FOLDER

        MISTER_IP = data['mister_ip']
        MISTER_USER = data['mister_user']
        MISTER_PASSWORD = data['mister_password']
        USE_PASSWORD = data['use_password']
        TARGET_GAME_FOLDER = data['target_game_folder']

        print("Launcher config loaded successfully.")
    else:
        print("No config file available.")

def ssh_cmd(cmd):

    if USE_PASSWORD == True:
        print('SSH: Login with password. Use private/public key auth if possible!')
        result = subprocess.run(['plink', '-batch', '-l', MISTER_USER, '-pw', MISTER_PASSWORD, MISTER_IP , cmd], 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
    else:
        print('SSH: Login using private/public key. Load key with pageant.')
        result = subprocess.run(['plink', '-batch', '-l', MISTER_USER, '-pw', MISTER_PASSWORD, MISTER_IP , cmd], 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)

def ssh_scp(src,target):
    exists = False
    if not FORCE_OVERWRITE:
        exists = ssh_test_path(target)
            
    if exists:
        print("SCP: File already exists on target, skipping. Use force flag to overwrite.")

    # TODO: Add pub/priv key auth
    if True and not exists:
        result = subprocess.run(['pscp', '-l', MISTER_USER, '-pw', MISTER_PASSWORD, src, f'{MISTER_IP}:{target}'], 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)

        print(f'SCP: Successfully copied {str(src)} to MiSTer.')

def ssh_test_path(path):
    # ssh host "test -e /path/to/file"
    # if [ $? -eq 0 ]; then
    # # your file exists
    # fi
    #cmd="echo \"load_core /media/fat/" + f'{TARGET_GAME_FOLDER}' + "/" + f'{mgl}' + "\" > /dev/MiSTer_cmd"
    cmd = f'test -e {path} && echo True || echo False'
    # TODO: Add pub/priv key auth
    if True:
        result = subprocess.run(['plink', '-batch', '-l', MISTER_USER, '-pw', MISTER_PASSWORD, MISTER_IP , cmd], 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)  

    if  result.stdout[:-1].decode("utf-8") == "True":
        return True
    else:
        return False

def write_mgl(platform, rom_path):
    # <mistergamedescription>
	#     <rbf>_console/genesis</rbf>
    #     <file delay="2" type="f" index="0" path="../../_mgl/__games/Sonic The Hedgehog 2 (World) (Rev A).md"/>
    # </mistergamedescription>

    rom_path = Path(rom_path)
    game_name = rom_path.stem
    mgl_path = Path("./temp/") / Path(game_name).with_suffix('.mgl')
    rbf = ""
    mgl_config = 'delay="2" type="f" index="0"'
    game_folder = ""

    if platform == 'genesis':
        rbf = "_console/genesis"
        game_folder = "Genesis"
    elif platform == 'snes':
        rbf = "_console/SNES"
        game_folder = "SNES"
    elif platform == 'nes':
        rbf = "_console/NES"
        game_folder = "NES"
        
    with mgl_path.open(mode='w') as f:
        f.write('<mistergamedescription>\n')
        f.write(f'\t<rbf>{rbf}</rbf>\n')
        f.write(f'\t<file {mgl_config} path="../../{TARGET_GAME_FOLDER}/{TARGET_ROM_SUB}/{game_folder}/{rom_path.name}"/>\n')
        f.write('</mistergamedescription>')

    ssh_scp(mgl_path, f'/media/fat/{TARGET_GAME_FOLDER}/')

    print(f'MGL: Created mgl file for platform {game_folder} and game "{game_name}"')

    return mgl_path

def copy_rom(platform, rom_path):
    #pscp -l root -pw 1 -r '.\ingest\Sonic the Hedgehog 2 (World) (Rev A).md' 10.23.0.61:/media/fat/_mgl/__games/
    
    if platform == 'genesis':
        mister_path = f'/media/fat/{TARGET_GAME_FOLDER}/{TARGET_ROM_SUB}/Genesis/'
        ssh_cmd(f'mkdir -p /media/fat/{TARGET_GAME_FOLDER}/{TARGET_ROM_SUB}/Genesis/')
    elif platform == 'snes':
        mister_path = f'/media/fat/{TARGET_GAME_FOLDER}/{TARGET_ROM_SUB}/SNES/'
        ssh_cmd(f'mkdir -p /media/fat/{TARGET_GAME_FOLDER}/{TARGET_ROM_SUB}/SNES/')
    elif platform == 'nes':
        mister_path = f'/media/fat/{TARGET_GAME_FOLDER}/{TARGET_ROM_SUB}/NES/'
        ssh_cmd(f'mkdir -p /media/fat/{TARGET_GAME_FOLDER}/{TARGET_ROM_SUB}/NES/')

    ssh_scp(rom_path, mister_path)

def play_rom(platform, rom_path, mgl):
    #plink -batch -l root -pw 1 10.23.0.61 'echo "load_core /media/fat/_mgl/Sonic the Hedgehog 2.mgl" > /dev/MiSTer_cmd'
    cmd="echo \"load_core /media/fat/" + f'{TARGET_GAME_FOLDER}' + "/" + f'{mgl}' + "\" > /dev/MiSTer_cmd"
    print(cmd)

    if platform == 'genesis':
        print('Launching ROM for: SEGA - GENESIS/MegaDrive')
    elif platform == 'snes':
        print('Launching ROM for: Super Nintendo Entertainment System')
    elif platform == 'nes':
        print('Launching ROM for: Nintendo Entertainment System')

    ssh_cmd(cmd)

def clean_up(mgl):
    mgl.unlink()

def launch_rom(platform, rom_path):
    mgl = write_mgl(platform, rom_path)
    copy_rom(platform, rom_path)
    play_rom(platform, rom_path, mgl.name)
    clean_up(mgl)

    ssh_test_path('/media/fat/_LaunchBox/test')

    
def main():
    args = parse_arguments()

    load_config()
    print(f'MiSTer IP address: {MISTER_IP}')

    launch_rom(args.p, args.r)

    #input("Press enter to exit")

if __name__ == '__main__':
    main()