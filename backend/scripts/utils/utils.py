import subprocess
from logging import getLogger
import os

from utils.logging import LOG_NAME

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

def create_directory(p):
    if not os.path.isdir(p):
        os.makedirs(p)
    return p

# https://gist.github.com/pritamd47/e7ddc49f25ae7f1b06c201f0a8b98348
# Clip time-series
def clip_ts(*tss):
    mint = max([min(ts.index) for ts in tss])
    maxt = min([max(ts.index) for ts in tss])

    # clipped_tss = [ts[mint:maxt] for ts in tss]
    clipped_tss = [ts.loc[(ts.index>=mint)&(ts.index<=maxt)] for ts in tss]

    return clipped_tss