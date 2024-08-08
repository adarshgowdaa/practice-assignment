import logging
from pymongo import MongoClient
from bson import ObjectId
import redis

class MongoDBRedisManager:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['redis_mongo']
        self.collection = self.db['collection']
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        
        # Setting up logging with time formatting
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def store_age_in_redis(self, age):
        try:
            self.logger.info(f"Storing documents with age {age} in Redis.")
            documents = self.collection.find({'age': age})
            for doc in documents:
                phone = doc['phone']
                self.redis_client.set(phone, str(doc))
            self.logger.info(f"Successfully stored documents with age {age} in Redis.")
        except Exception as e:
            self.logger.error(f"Error storing documents in Redis: {e}")

    def update_age_in_redis(self, new_age):
        try:
            self.logger.info(f"Updating age to {new_age} in Redis.")
            keys = self.redis_client.keys()
            for key in keys:
                doc = eval(self.redis_client.get(key))
                doc['age'] = new_age
                self.redis_client.set(key, str(doc))
            self.logger.info(f"Successfully updated age to {new_age} in Redis.")
        except Exception as e:
            self.logger.error(f"Error updating age in Redis: {e}")

    def write_back_to_mongo(self):
        try:
            self.logger.info("Writing updated documents back to MongoDB.")
            keys = self.redis_client.keys()
            for key in keys:
                doc = eval(self.redis_client.get(key))
                self.collection.update_one({'phone': key}, {'$set': {'age': doc['age']}})
            self.logger.info("Successfully wrote updated documents back to MongoDB.")
        except Exception as e:
            self.logger.error(f"Error writing back to MongoDB: {e}")

if __name__ == "__main__":
    manager = MongoDBRedisManager()
    manager.store_age_in_redis(25)
    manager.update_age_in_redis(30)
    manager.write_back_to_mongo()