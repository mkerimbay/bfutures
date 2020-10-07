import logging
from logging import config
import os

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


print(os.listdir())
logging.config.fileConfig('./_logger.conf')
logger = logging.getLogger(__name__)
print(__name__)
