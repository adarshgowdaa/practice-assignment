import logging
import time
from pymongo import MongoClient
from faker import Faker

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoDBHandler:
    def __init__(self, db_name, collection_name, uri="mongodb://localhost:27017/"):
        try:
            self.client = MongoClient(uri)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            logger.info(f"Connected to MongoDB database: {db_name}, collection: {collection_name}")
        except Exception as e:
            logger.exception(f"Error connecting to MongoDB: {e}")
            raise

    def create_document(self, data):
        try:
            start_time = time.time()
            result = self.collection.insert_one(data)
            duration = time.time() - start_time
            logger.info(f"Document created with ID: {result.inserted_id} in {duration:.4f} seconds")
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error creating document: {e}")
            return None
        
    def insert_many_documents(self, data_list):
        try:
            start_time = time.time()
            result = self.collection.insert_many(data_list)
            duration = time.time() - start_time
            logger.info(f"Documents created with IDs: {result.inserted_ids} in {duration:.4f} seconds")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"Error creating documents: {e}")
            return None
        
    def read_documents(self, query):
        try:
            start_time = time.time()
            documents = self.collection.find(query)
            duration = time.time() - start_time
            logger.info(f"Documents read with query: {query} in {duration:.4f} seconds")
            return list(documents)
        except Exception as e:
            logger.error(f"Error reading documents: {e}")
            return None
        
    def read_all_documents(self):
        try:
            start_time = time.time()
            documents = self.collection.find()
            duration = time.time() - start_time
            logger.info(f"All documents read in {duration:.4f} seconds")
            return list(documents)
        except Exception as e:
            logger.error(f"Error reading all documents: {e}")
            return None
        
    def count_documents(self, query):
        try:
            start_time = time.time()
            count = self.collection.count_documents(query)
            duration = time.time() - start_time
            logger.info(f"Documents counted with query: {query} in {duration:.4f} seconds")
            return count
        except Exception as e:
            logger.error(f"Error counting documents: {e}")
            return None
        
    def count_all_documents(self):
        try:
            start_time = time.time()
            count = self.collection.count_documents({})
            duration = time.time() - start_time
            logger.info(f"All documents counted in {duration:.4f} seconds")
            return count
        except Exception as e:
            logger.error(f"Error counting all documents: {e}")
            return None
        

    def read_document(self, field):
        try:
            start_time = time.time()
            document = self.collection.find_one(field)
            duration = time.time() - start_time
            logger.info(f"Document read with field: {field} in {duration:.4f} seconds")
            return document
        except Exception as e:
            logger.error(f"Error reading document: {e}")
            return None

    def update_document(self, query, updated_data):
        try:
            start_time = time.time()
            result = self.collection.update_one(query, {"$set": updated_data})
            duration = time.time() - start_time
            logger.info(f"Document updated with query: {query} in {duration:.4f} seconds, Modified count: {result.modified_count}")
            return result.modified_count
        except Exception as e:
            logger.error(f"Error updating document: {e}")
            return None


    def delete_document(self, query):
        try:
            start_time = time.time()
            result = self.collection.delete_one(query)
            duration = time.time() - start_time
            logger.info(f"Document deleted with query: {query} in {duration:.4f} seconds, Deleted count: {result.deleted_count}")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            return None

    def delete_documents(self, query):
        try:
            start_time = time.time()
            result = self.collection.delete_many(query)
            duration = time.time() - start_time
            logger.info(f"Documents deleted with query: {query} in {duration:.4f} seconds, Deleted count: {result.deleted_count}")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting documents: {e}")
            return None
        
def generate_random_data(num_entries):
    faker = Faker()
    data_list = []
    for _ in range(num_entries):
        data_list.append({
            "firstName": faker.first_name(),
            "lastName": faker.last_name(),
            "email": faker.email(),
            "phone": faker.phone_number(),
            "age": faker.random_int(min=18, max=80),
        })
    return data_list


# Example usage
if __name__ == "__main__":
    db_handler = MongoDBHandler("test", "collection")

    # Read the document
    document = db_handler.read_document({"name": "Aimee Boyd"})
    print("Read document:", document)