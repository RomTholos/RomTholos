import subprocess, os, glob

# Silently create one single par2 archive with 5% redundancy.
def create_par2(path, override=False):
    if override == True:
        for f in glob.glob(path+'*.par2'):
            os.unlink(f)
    try:
        result = subprocess.run(["par2", "create", "-r5", "-n1", "-q", "-q", path], capture_output=True, check=True)
        for f in glob.glob(path+'.*.par2'):
            os.replace(f, path+'.par2')
        return path+'.par2'
    except subprocess.CalledProcessError as e:
        #print(e.output)
        print('Could not create file. Already there?')

def verify_par2(path):
    result = subprocess.run(["par2", "verify", "-q", "-q", path], capture_output=True, check=True)
    if result.returncode == 0:
        return True
    else:
        return False

#print(create_par2("FILE"))
#print(create_par2("FILE",override=True))
#print(verify_par2("FILE"))