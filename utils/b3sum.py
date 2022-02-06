import subprocess

def getBlake3Sum(path):
    result = subprocess.run(["b3sum", "--no-names", str(path)], capture_output=True, check=True)
        
    return [result.returncode, result.stdout[:-1].decode("utf-8")]
    
def get_b3sum(path):
    result = subprocess.run(["b3sum", "--no-names", str(path)], capture_output=True, check=True)
        
    return result.stdout[:-1].decode("utf-8").upper()
    
#print(getBlake3Sum("FILE")[1])