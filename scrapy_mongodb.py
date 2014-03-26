"""
scrapy-mongodb - MongoDB pipeline for Scrapy

Homepage: https://github.com/sebdah/scrapy-mongodb
Author: Sebastian Dahlgren <sebastian.dahlgren@gmail.com>
License: Apache License 2.0 <http://www.apache.org/licenses/LICENSE-2.0.html>

Copyright 2013 Sebastian Dahlgren

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import datetime

from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference
from bson.json_util import dumps

from scrapy import log


VERSION = '0.6.2'


def not_set(string):
    """ Check if a string is None or ''

    :returns: bool - True if the string is empty
    """
    if string is None:
        return True
    elif string == '':
        return True
    return False


class MongoDBPipeline():
    """ MongoDB pipeline class """
    # Default options
    config = {
        'uri': 'mongodb://localhost:27017',
        'fsync': False,
        'write_concern': 0,
        'database': 'scrapy-mongodb',
        'collection': 'items',
        'replica_set': None,
        'unique_key': None,
        'buffer': None,
        'append_timestamp': False,
    }

    # Item buffer
    current_item = 0
    item_buffer = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        """ Constructor """
        self.settings = settings

        # Configure the connection
        self.configure()

        if self.config['replica_set'] is not None:
            connection = MongoReplicaSetClient(
                self.config['uri'],
                replicaSet=self.config['replica_set'],
                w=self.config['write_concern'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                self.config['uri'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the collection
        database = connection[self.config['database']]
        self.collection = database[self.config['collection']]
        log.msg('Connected to MongoDB {0}, using "{1}/{2}"'.format(
            self.config['uri'],
            self.config['database'],
            self.config['collection']))

        # Ensure unique index
        if self.config['unique_key']:
            self.collection.ensure_index(self.config['unique_key'], unique=True)
            log.msg('Ensuring index for key {0}'.format(
                self.config['unique_key']))

    def configure(self):
        """ Configure the MongoDB connection """
        # Handle deprecated configuration
        if not not_set(self.settings['MONGODB_HOST']):
            log.msg(
                'DeprecationWarning: MONGODB_HOST is deprecated',
                level=log.WARNING)
            mongodb_host = self.settings['MONGODB_HOST']

            if not not_set(self.settings['MONGODB_PORT']):
                log.msg(
                    'DeprecationWarning: MONGODB_PORT is deprecated',
                    level=log.WARNING)
                self.config['uri'] = 'mongodb://{0}:{1:i}'.format(
                    mongodb_host,
                    self.settings['MONGODB_PORT'])
            else:
                self.config['uri'] = 'mongodb://{0}:27017'.format(mongodb_host)

        if not not_set(self.settings['MONGODB_REPLICA_SET']):
            if not not_set(self.settings['MONGODB_REPLICA_SET_HOSTS']):
                log.msg(
                    (
                        'DeprecationWarning: '
                        'MONGODB_REPLICA_SET_HOSTS is deprecated'
                    ),
                    level=log.WARNING)
                self.config['uri'] = 'mongodb://{0}'.format(
                    self.settings['MONGODB_REPLICA_SET_HOSTS'])

        # Set all regular options
        options = [
            ('uri', 'MONGODB_URI'),
            ('fsync', 'MONGODB_FSYNC'),
            ('write_concern', 'MONGODB_REPLICA_SET_W'),
            ('database', 'MONGODB_DATABASE'),
            ('collection', 'MONGODB_COLLECTION'),
            ('replica_set', 'MONGODB_REPLICA_SET'),
            ('unique_key', 'MONGODB_UNIQUE_KEY'),
            ('buffer', 'MONGODB_BUFFER_DATA'),
            ('append_timestamp', 'MONGODB_ADD_TIMESTAMP'),
        ]

        for key, setting in options:
            if not not_set(self.settings[setting]):
                self.config[key] = self.settings[setting]

        # Check for illegal configuration
        if self.config['buffer'] and self.config['unique_key']:
            log.msg(
                (
                    'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                    'and MONGODB_UNIQUE_KEY is not supported'
                ),
                level=log.ERROR)
            raise SyntaxError(
                (
                    'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                    'and MONGODB_UNIQUE_KEY is not supported'
                ))

    def process_item(self, item, spider):
        """ Process the item and add it to MongoDB

        :type item: Item object
        :param item: The item to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        if self.config['buffer']:
            self.current_item += 1
            item = dict(item)

            if self.config['append_timestamp']:
                item['created_at'] = datetime.datetime.utcnow() 

            self.item_buffer.append(item)

            if self.current_item == self.config['buffer']:
                self.current_item = 0
                return self.insert_item(self.item_buffer, spider)

            else:
                return item

        return self.insert_item(item, spider)

    def insert_item(self, item, spider):
        """ Process the item and add it to MongoDB

        :type item: (Item object) or [(Item object)]
        :param item: The item(s) to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        if not isinstance(item, list):
            item = dict(item)

            if self.config['append_timestamp']:
                item['created_at'] = datetime.datetime.utcnow() 


        if self.config['unique_key'] is None:
            try:
                print "========================="
                # db_items = self.collection.find_one({'url': str(item['url'])})
                db_items_temp = self.collection.find({'url': str(item['url'])}).sort('created_at', -1)

                print ":::::::::::::::::"
                print db_items_temp.count()
                print "::::::::::::::::" 
                if db_items_temp.count() > 0:
                    db_items = db_items_temp.next() 
                else:
                    db_items = ""

                is_dup = True

                if db_items:
                    print item
                    print '--------------------'
                    print db_items
                    print ">>>>>>>>>>>>>>>"

                    print item['title']


                    if item['title'] != db_items['title']:
                        print '----title----'
                        is_dup = False
                    
                    if item['domain'] != db_items['domain']:
                        print '---domain---'
                        is_dup = False

                    if item['desc'] != db_items['desc']:
                        print '---desc----'
                        is_dup = False

                    if item['currency'] != db_items['currency']:
                        print '---currency---'
                        is_dup = False

                    if item['price'] != db_items['price']:
                        print '---price---'
                        is_dup = False

                    if item['options'] != db_items['options']:
                        print '---options---'
                        is_dup = False

                    if item['images'] != db_items['images']:
                        print '---images---'
                        is_dup = False


                    if not is_dup:
                        item['status'] = "update"
                        self.collection.insert(item, continue_on_error=True)

                    print db_items['options']

                    print "++++++"
                    print is_dup

                   
                else:
                    item['status'] = "create"
                    self.collection.insert(item, continue_on_error=True)
                # print dumps(c)

                print "========================="
                
            except errors.DuplicateKeyError:
                pass

        else:
            self.collection.update(
                {
                    self.config['unique_key']: item[self.config['unique_key']]
                },
                item,
                upsert=True)

        log.msg(
            'Stored item(s) in MongoDB {0}/{1}'.format(
                self.config['database'], self.config['collection']),
            level=log.DEBUG,
            spider=spider)

        return item
