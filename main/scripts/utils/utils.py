import os
import math

def round_pixels(x):
    if((x*10)%10<5):
        x=int(x)
    else:
        x=int(x)+1
    return(x)


def create_directory(p,path_return=False):
    if not os.path.isdir(p):
        os.makedirs(p)
    if path_return:
        return p

def round_up(n, decimals=0): 
    multiplier = 10 ** decimals 
    if n>0:
        return math.ceil(n * multiplier) / multiplier
    else:
        return -math.ceil(abs(n) * multiplier) / multiplier