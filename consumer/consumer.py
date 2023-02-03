import redis
import boto3
import os
from pathlib import Path                                                        
import platform                                                                 
import logging                                                                  
import logging.config
from ast import literal_eval
import time
import multiprocessing 
from multiprocessing import Process 
from concurrent.futures import ThreadPoolExecutor
import threading
from threading import current_thread

BUCKET_NAME = os.getenv("BUCKET_NAME" , "binguli-public")
PROCESS_COUNT = int(os.getenv("PROCESS_COUNT" , "2"))
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
BUFFER_NAME = os.getenv("BUFFER_NAME", "buffer")
QUEUE_NAME = os.getenv("QUEUE_NAME", "original")
OBJECT_QUEUE = os.getenv("OBJECT_QUEUE", "uploaded-replicated")
BUFFER_COUNT = int(os.getenv("BUFFER_COUNT" , 10 ))
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY", "ccLG5Y5jiza3BjyMLSNr")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID", "jRTipnGNRKQti")
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://127.0.0.1:9000")
# Log.       
DESTINATION_DIR = Path("/OSS")                                                                                                                     
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")                                     
os.makedirs(                                                                    
    f"{DESTINATION_DIR.absolute().as_posix()}/uploader_worker_logs/{platform.node()}/",  
    exist_ok=True,                                                              
)                                                                               
LOG_FILE = os.getenv(                                                           
    "LOG_FILE",                                                                 
    f"{DESTINATION_DIR.absolute().as_posix()}/uploader_worker_logs/{platform.node()}/{platform.node()}.log",
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


    def upload(self, filePath):
        uploaded = False
        logger.debug("uploading the file %s %s", filePath , threading.current_thread().name)
        object_info = filePath.split("/", 3)
        
        try:
            #self.s3.upload_file(filePath, BUCKET_NAME, object_info[3] , ExtraArgs= self.ExtraArgs)
            self.s3.upload_file(filePath, BUCKET_NAME, object_info[3])
            uploaded = True 
        except Exception as e:
            logger.error("error in upload file %s", e)
        return uploaded


class Consumer:
    def __init__(self):
     
        self.db = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True
        )
        self.s3=S3();

    def pop_from_redis(self, queueName, count=None):
        if count != None:

             files = self.db.rpop(queueName, count)
        else:
             files = self.db.rpop(queueName)
        logger.debug("file poped from redis from process %s", files )
        return files

    def push_to_buffer(self, files):
        for f in files:
            self.db.lpush(self.buffer_name, f)

    def handle_buffer(self, queue_name):
        while self.db.llen(queue_name) != 0:
            fileName = self.pop_from_redis(queue_name)
            isUploaded = self.s3.upload(fileName)
            if not isUploaded:
                self.db.lpush(self.buffer_name , fileName)
            else:
                self.db.lpush(OBJECT_QUEUE , fileName)

    def check_buffer(self):

        if self.db.llen(self.buffer_name) > 0:
            self.handle_buffer(self.buffer_name)

    def run(self):
        print("sag")
        self.buffer_name = BUFFER_NAME + threading.current_thread().name
        while True:
             self.check_buffer()
             while self.db.llen(QUEUE_NAME) > 0:

                 if self.db.llen(QUEUE_NAME) > BUFFER_COUNT:
                     files = self.pop_from_redis(QUEUE_NAME, BUFFER_COUNT)
                     self.push_to_buffer(files)
                     self.handle_buffer(self.buffer_name)

                 else:
                     self.handle_buffer(QUEUE_NAME)


if __name__ == "__main__":
    try:
      
      
      
        #t1 = threading.Thread(name='thread1', target=Consumer().run)
        #t2 = threading.Thread(name='thread2', target=Consumer().run)
        #t3 = threading.Thread(name='thread3', target=Consumer().run)
        #t4 = threading.Thread(name='thread4', target=Consumer().run)
        #t5 = threading.Thread(name='thread5', target=Consumer().run)

        #t1.start()
        #t2.start()
        #t3.start()
        #t4.start()
        #t5.start()
        #t1.join()
        #t2.join()
        #t3.join()
        #t4.join()
        #t5.join()

        with ThreadPoolExecutor(max_workers=PROCESS_COUNT) as exe:
           exe.map(Consumer().run , range(1,PROCESS_COUNT + 1))

    except KeyboardInterrupt:
        print("\rGoodbye!")
        #break
    except Exception as e:
        time.sleep(10)
        print("error" , e)

        #continue
    #while True:
       #try:
       #    #consumer.run()
       #except KeyboardInterrupt:
       #    print("\rGoodbye!")
       #    break
       #except Exception:
       #    time.sleep(10)
       #    continue

