import glob
import os
import redis
from pathlib import Path                                                        
import platform                                                                 
import logging                                                                  
import logging.config  
import time

OSS_PATH = os.getenv("OSS_PATH", "/OSS/digikala-public")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "original")
OBJECTS_NUMBER = int(os.getenv("OBJECTS_NUMBER", "10"))
OBJECTS_FILE = Path("/OSS/objects.txt")


#log
DESTINATION_DIR = Path("/OSS")                                                    
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")                                     
os.makedirs(                                                                    
    f"{DESTINATION_DIR.absolute().as_posix()}/worker_logs/{platform.node()}/",  
    exist_ok=True,                                                              
)                                                                               
LOG_FILE = os.getenv(                                                           
    "LOG_FILE",                                                                 
    f"{DESTINATION_DIR.absolute().as_posix()}/worker_logs/{platform.node()}/{platform.node()}.log",
)                                                                               
LOG_FILE_MAX_BYTES = int(os.getenv("LOG_FILE_MAX_BYTES", "2147483648"))         
LOG_FILE_BACKUP_COUNT = int(os.getenv("LOG_FILE_BACKUP_COUNT", "1000"))         
LOGGING = {                                                                     
    "version": 1,                                                               
    "disable_existing_loggers": True,                                           
    "formatters": {                                                             
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },                                                                          
    "handlers": {                                                               
        "default": {                                                            
            "formatter": "standard",                                            
            "class": "logging.StreamHandler",                                   
            "stream": "ext://sys.stderr",                                       
        },                                                                      
        "rotating_file_handler": {                                              
            "class": "logging.handlers.RotatingFileHandler",                    
            "filename": LOG_FILE,                                               
            "formatter": "standard",                                            
            "mode": "a",                                                        
            "maxBytes": LOG_FILE_MAX_BYTES,                                     
            "backupCount": LOG_FILE_BACKUP_COUNT,                               
        },                                                                      
    },                                                                          
    "loggers": {                                                                
        "": {"handlers": ["default", "rotating_file_handler"], "propagate": False},
    },                                                                          
}                                                                               
logging.config.dictConfig(LOGGING)                                              
logger = logging.getLogger(platform.node())                                     
logger.setLevel(LOG_LEVEL) 

class Producer:
    def __init__(self):

        self.db = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
        )
        self.totalCount = OBJECTS_NUMBER
        self.count = 0
        self.objects_file=open(OBJECTS_FILE , 'w')

    def push_to_redis(self, fileName):
        self.db.lpush(QUEUE_NAME, fileName)

    def get_list(self):
        try:
            for currentpath, folders, files in os.walk(OSS_PATH):
                for file in files:
                    print("count is "+ str(self.count))
                    if ( self.count < self.totalCount):
                         self.count += 1
                         logger.debug("push to redis %s", os.path.join(currentpath, file))
                         self.push_to_redis(os.path.join(currentpath, file))
                         self.objects_file.write(os.path.join(currentpath, file).split("/", 3)[3] + '\n')
                    else:
                      return
        except Exception as e:
            print("ridi %s" , e)
            
    def run(self):
        self.get_list()


if __name__ == "__main__":
    try:
        Producer().run()
    except KeyboardInterrupt:
        print("\rGoodbye!")
    except Exception:
        time.sleep(10)

