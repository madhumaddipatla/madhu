import logging
import logging.handlers
import time
from ConfigParser import SafeConfigParser

CONFIGURATION_FILE = "config.ini"
parser = SafeConfigParser()
parser.read(CONFIGURATION_FILE)
BASEPATH = parser.get("FileReader", 'basepath')
log_level = parser.get('FileReader','log_level')
LOG_FILE = BASEPATH + "glens"

logger = logging.getLogger('glens')
hdlr_uploader = logging.FileHandler(LOG_FILE + "_" + time.strftime("%Y%m%d")+'.log')
hdlr_service = logging.FileHandler(LOG_FILE + "_" + time.strftime("%Y%m%d")+'.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s',"%Y-%m-%d %H:%M:%S")
hdlr_uploader.setFormatter(formatter)
hdlr_service.setFormatter(formatter)
logger.addHandler(hdlr_uploader)
logger.setLevel(log_level)
logger.debug('Logger Initialized')
