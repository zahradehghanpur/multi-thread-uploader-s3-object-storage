import platform                                                                 
import logging                                                                  
import logging.config  
import csv
import os
from pathlib import Path
import time
import redis
import boto3
OSS_PATH = os.getenv("OSS_PATH", "/OSS/")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "original")
BUCKET_NAME = os.getenv("BUCKET_NAME" , "binguli-public-blaum")
STORAGE_CLASS = os.getenv("STORAGE_CLASS", "blaum_roth" )
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY", "ccLG5Y5jie0Q9vM6vdopiza3BjyMLSNr")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID", "jRTiWBgpnGNRKQti")
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://127.0.0.1:9000")

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


class S3:

    def __init__(self):

        self.s3 = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            aws_access_key_id=ACCESS_KEY_ID,
        )

        self.s3_resource = boto3.resource(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_secret_access_key=SECRET_ACCESS_KEY,
            aws_access_key_id=ACCESS_KEY_ID,
        )
        self.ExtraArgs = {'StorageClass': STORAGE_CLASS}

    def upload(self, objectName):
        logger.debug("uploading the file %s", objectName)
        #object_info = filePath.split("/", 3)
        filePath = "/OSS/digikala-public/{}".format( objectName)
        logger.debug(self.ExtraArgs)
        logger.debug(filePath)
        logger.debug(BUCKET_NAME)
        try:
            self.s3.upload_file(filePath, BUCKET_NAME, objectName , ExtraArgs= self.ExtraArgs)
        except Exception as e:
            logger.error("error in upload file %s", e)


class Producer:
    def __init__(self):
        print("ahh")
        #self.db = redis.Redis(
        #    host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
        #)
        self.s3=S3()
        print("goh")
    def push_to_redis(self, fileName):
        print("giri kardima")
        #self.db.lpush(QUEUE_NAME, fileName)

    def read_csv(self):
        filename = open('failures_1673709644.635706.csv', 'r')
        file = csv.DictReader(filename, delimiter=',')
        for col in file:
            print(col['Name'].split("/",2)[2])
            print("ehsan")
            self.s3.upload(col['Name'].split("/",2)[2])
            #self.push_to_redis(col['goh'].split("?",2)[0])
    
        
    def run(self):
         self.read_csv()
        

if __name__ == "__main__":
    try:
        Producer().run()
    except KeyboardInterrupt:
        print("\rGoodbye!")
    except Exception:
        time.sleep(10)

