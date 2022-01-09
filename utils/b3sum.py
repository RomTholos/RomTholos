import subprocess

def getBlake3Sum(path):
    result = subprocess.run(["b3sum", "--no-names", path], capture_output=True, check=True)
        
    return [result.returncode, result.stdout[:-1].decode("utf-8")]
    
#print(getBlake3Sum("FILE")[1])