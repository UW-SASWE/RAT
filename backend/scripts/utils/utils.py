import subprocess
from utils.logging import LOG_NAME
from logging import getLogger


log = getLogger(LOG_NAME)



def run_command(args, **kwargs):
    """Safely runs a command, logs and returns the returncode silently in case of no error. 
    Otherwise, raises an Exception
    """
    if isinstance(args, list):
        log.debug("Running command: %s", " ".join(args))
    else:
        log.debug("Running command: %s", args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)

    with p.stdout:
        for line in iter(p.stdout.readline, b''): # b'\n'-separated lines
            log.debug("%r", line)
        
    exitcode = p.wait()

    if exitcode == 0:
        log.debug("Finished routing successfully: EXIT CODE %s", exitcode)
    else:
        log.error("ERROR Occurred with exit code: %s", exitcode)
        raise Exception
    
    return exitcode