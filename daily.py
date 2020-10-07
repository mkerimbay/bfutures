import os
from helper import logger
from _credentials import *
abspath = os.path.abspath(__file__)
curr_path = os.path.dirname(abspath)
os.chdir(curr_path)
#logger.info('hello world')
print(key)
