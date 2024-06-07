"""DB Module"""

import os

import pymongo

USER = os.environ.get("MONGO_USER")
PASS = os.environ.get("MONGO_PASS")


client = pymongo.MongoClient(f"mongodb://{USER}:{PASS}@localhost:27017/")


def get_collection():
    """Получаем sample_collection"""
    db = client.get_database("test")
    collection = db.get_collection("sample_collection")
    return collection
