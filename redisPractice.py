import redis
import logging
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO)

class RedisDB:

    def __init__(self):
        self.redis_conn = redis.Redis(host='localhost', port=6379, db=0)

    def redis_set_value(self, key, value):
        try:
            start_time = time.time()
            result = self.redis_conn.set(key, value)
            duration = time.time() - start_time
            logging.info(f"SET - Key: {key}, Value: {value}, Duration: {duration:.4f} seconds")
            return result
        except Exception as e:
            logging.error(f"Error setting value for key {key}: {e}")
            return None

    def redis_get_value(self, key):
        try:
            start_time = time.time()
            value = self.redis_conn.get(key)
            duration = time.time() - start_time
            logging.info(f"GET - Key: {key}, Value: {value}, Duration: {duration:.4f} seconds")
            return value
        except Exception as e:
            logging.error(f"Error getting value for key {key}: {e}")
            return None

    def redis_delete_value(self, key):
        try:
            start_time = time.time()
            result = self.redis_conn.delete(key)
            duration = time.time() - start_time
            logging.info(f"DELETE - Key: {key}, Duration: {duration:.4f} seconds")
            return result
        except Exception as e:
            logging.error(f"Error deleting key {key}: {e}")
            return None

    def redis_set_value_and_expiry(self, key, value, expiry):
        try:
            start_time = time.time()
            result = self.redis_conn.setex(key, expiry, value)
            duration = time.time() - start_time
            logging.info(f"SET with Expiry - Key: {key}, Value: {value}, Expiry: {expiry}, Duration: {duration:.4f} seconds")
            return result
        except Exception as e:
            logging.error(f"Error setting value with expiry for key {key}: {e}")
            return None

    def redis_list_push(self, key, value):
        try:
            start_time = time.time()
            result = self.redis_conn.lpush(key, value)
            duration = time.time() - start_time
            logging.info(f"LPUSH - Key: {key}, Value: {value}, Duration: {duration:.4f} seconds")
            return result
        except Exception as e:
            logging.error(f"Error pushing to list for key {key}: {e}")
            return None

    def redis_list_pop(self, key):
        try:
            start_time = time.time()
            value = self.redis_conn.lpop(key)
            duration = time.time() - start_time
            logging.info(f"LPOP - Key: {key}, Value: {value}, Duration: {duration:.4f} seconds")
            return value
        except Exception as e:
            logging.error(f"Error popping from list for key {key}: {e}")
            return None

    def redis_list_get_all(self, key):
        try:
            start_time = time.time()
            values = self.redis_conn.lrange(key, 0, -1)
            duration = time.time() - start_time
            logging.info(f"LRANGE - Key: {key}, Values: {values}, Duration: {duration:.4f} seconds")
            return values
        except Exception as e:
            logging.error(f"Error getting all list values for key {key}: {e}")
            return None

if __name__ == "__main__":
    db = RedisDB()
    
    # String operations
    db.redis_set_value("name", "Alice")
    print(db.redis_get_value("name"))
    db.redis_set_value_and_expiry("temp", "data", 10)
    print(db.redis_get_value("temp"))
    db.redis_delete_value("temp")

    # List operations
    db.redis_list_push("numbers", 1)
    db.redis_list_push("numbers", 2)
    db.redis_list_push("numbers", 3)
    print(db.redis_list_get_all("numbers"))
    db.redis_list_pop("numbers")
    print(db.redis_list_get_all("numbers"))
