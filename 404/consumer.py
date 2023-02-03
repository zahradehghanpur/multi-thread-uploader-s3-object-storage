import redis
import boto3
import os
from pathlib import Path                                                        
import platform                                                                 
import logging                                                                  
import logging.config
import botocore.config
import random
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6380"))
BUFFER_NAME = "buffer+{}".format(str(random.randint(0,1000)))
QUEUE_NAME = os.getenv("QUEUE_NAME", "original")
BUFFER_COUNT = int(os.getenv("BUFFER_COUNT" , 1000 ))
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY", "ccLG5Y5jie0Q9vM6vdopiza3BjyMLSNr")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID", "jRTiWBgpnGNRKQti")
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://127.0.0.1:9000")
BUCKET_NAME = os.getenv("BUCKET_NAME" , "binguli-public-blaum")
STORAGE_CLASS = os.getenv("STORAGE_CLASS", "blaum_roth" )

# Log.       
DESTINATION_DIR = Path("/OSS2")                                                                                                                     
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

    #def upload(self, filePath):
    #    logger.debug("uploading the file %s", filePath)
    #    object_info = filePath.split("/", 3)
    #    try:
    #        self.s3.upload_file(filePath, object_info[2], object_info[3])
    #    except Exception as e:
    #        logger.error("error in upload file %s", e)
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

    def download(self, objectName , fullPath):
        isDownloaded = False
        #logger.debug("downloading the file %s", fullPath)
        try:
            self.s3.download_file(BUCKET_NAME, objectName, fullPath)
            isDownloaded = True
            logger.debug("downloaded the file %s", fullPath)
            
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.error(
                    "error downloading 404 bucket: %s object: %s error: %s",
                    BUCKET_NAME,
                    objectName,
                    e,
                )
        except Exception as e:
            logger.error(
                "Error downloading bucket: %s object: %s error: %s",
                BUCKET_NAME,
                objectName,
                e,
            )
        return isDownloaded

class Consumer:
    def __init__(self):
     
        self.db = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
        )
        self.s3=S3();

    def pop_from_redis(self, queueName, count=None):
        if count != None:
            files = self.db.rpop(queueName, count)
            logger.debug("file poped from redis %s", files )
            print(files)
        else:
            files = self.db.rpop(queueName)
        return files

    def push_to_buffer(self, files):
        for f in files:
            self.db.lpush(BUFFER_NAME, f)

    def handle_buffer(self, queue_name):
        try:
            while self.db.llen(queue_name) != 0:
                objectName = self.pop_from_redis(queue_name)
                fullPath = DESTINATION_DIR / BUCKET_NAME / objectName
                #if not fullPath.parent.exists():
                    #fullPath.parent.mkdir(parents=True, exist_ok=True)
                #isDownloaded = self.s3.download(str(objectName) , fullPath)
                #if isDownloaded:
                self.s3.upload(objectName)
                #else:
                   #self.push_to_buffer(objectName)
        except Exception as e:
             logger.error("ridi %s", e)

    def check_buffer(self):

        if self.db.llen(BUFFER_NAME) > 0:
            self.handle_buffer(BUFFER_NAME)

    def run(self):
        self.check_buffer()
        while self.db.llen(QUEUE_NAME) > 0:

            if self.db.llen(QUEUE_NAME) > BUFFER_COUNT:
                files = self.pop_from_redis(QUEUE_NAME, BUFFER_COUNT)
                self.push_to_buffer(files)
                self.handle_buffer(BUFFER_NAME)

            else:
                self.handle_buffer(QUEUE_NAME)


if __name__ == "__main__":
    consumer = Consumer()
    #while True:
    try:
        consumer.run()
    except KeyboardInterrupt:
        print("\rGoodbye!")
        #break
    except Exception:
        print("\rRIDII!")
        #time.sleep(10)
        #continue

