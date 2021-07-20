import subprocess


def run_command(cmd):
    """Safely runs a command, and returns the returncode silently in case of no error. Otherwise,
    raises an Exception
    """
    res = subprocess.run(cmd, check=True, capture_output=True)
    
    if res.returncode != 0:
        print(f"Error with return code {res.returncode}")
        raise Exception
    return res.returncode
