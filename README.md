# enron-agg-fw
Sample MongoDB aggregation framework usage on the public Enron Mail database

Overview
--------

Using the Enron sample database, contains several queries using MongoDB Aggregation Framework and a small python app using faceting.
Requires the specific Enron DB, as it is modified from the original (recipients are in an array and dates are converted to ISO 8601)

**Please note that this repo is for training and understanding only**

Installation
------------

Uncompress and import the database on your local MongoDB node :

    $ mongorestore

This creates the "enron" database, and the "messages" collection.

To run the Python script you need pymongo installed :

    $ pip install pymongo
    
Usage
-----

simply copy and paste the variables and queries in Mongo Shell, or run

    $ python unusualMailUsage.py
