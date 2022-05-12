import os

def round_pixels(x):
    if((x*10)%10<5):
        x=int(x)
    else:
        x=int(x)+1
    return(x)


def create_directory(p):
    if not os.path.isdir(p):
        os.makedirs(p)
    return p