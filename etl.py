# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

from prettytable import PrettyTable

# PART 1
# Creating list of filepaths to process original event csv data files

# Checking your current working directory
print(os.getcwd())
# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'
# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    print(file_path_list)
    

# Processing the files to create the data file csv that will be used for Apache Casssandra tables    

# Initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# For every filepath in the file path list 
for f in file_path_list:

# Reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
# Extracting each data row one by one and append it        
        for line in csvreader:            
            full_data_rows_list.append(line) 
            
# Creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

# New csv event_datafile_new_from_etlpy for testing
with open('event_datafile_new_from_etlpy.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# Check the number of rows in your csv file
with open('event_datafile_new_from_etlpy.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
    
    
# PART 2
# Creating a Cluster
# This should make a connection to a Cassandra instance your local machine 

# (127.0.0.1)
from cassandra.cluster import Cluster
cluster = Cluster()
# To establish connection and begin executing queries, need a session
session = cluster.connect()


# Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)
    

# CREATE TABLES
def create_tables():
    create_statement = "CREATE TABLE IF NOT EXISTS "
    field_template = "(%s, "
    keys_template = "PRIMARY KEY (%s))"
    
    queries_data = {
        "song_length_session": {
            "fields": ["sessionId int", "itemInSession int", "artist text", "song text", "length double"],
            "primary_keys": ["sessionId", "itemInSession"]
            
        },
        "song_playlist_session": {
            "fields": [
                "userId int", "sessionId int", "itemInSession int", 
                "artist text", "song text", 
                "firstName text", "lastName text", 

            ],
            "primary_keys": ["userId", "sessionId", "itemInSession"]
            
        },
        "user_song": {
            "fields": ["song text", "userId int","firstName text", "lastName text"],
            "primary_keys": ["song", "userId"]
            
        }
    }

    
    for table in queries_data.keys():
        fields_statement = (field_template % ",".join(queries_data.get(table).get("fields")))
        keys_statement = (keys_template % ",".join(queries_data.get(table).get("primary_keys")))
        query = create_statement + table + fields_statement + keys_statement          
        
        try:
            session.execute(query)
        except Exception as e:
            print(e)    

create_tables()


# We have provided part of the code to set up the CSV file.
def insert_table():
    file = 'event_datafile_new_from_etlpy.csv'
    
    insert_statements = [
        "INSERT INTO song_length_session (sessionId, itemInSession, artist, length, song) VALUES(%s, %s, %s, %s, %s)",
        "INSERT INTO song_playlist_session (userId, sessionId, itemInSession, artist, firstName, lastName, song) VALUES(%s, %s, %s, %s, %s, %s, %s)",
        "INSERT INTO user_song (song, userId, firstName, lastName) VALUES(%s, %s, %s, %s)"
    ]
    
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            for queryIndex, query in enumerate(insert_statements):
                if queryIndex == 0:
                    session.execute(query, (int(line[8]), int(line[3]), line[0], float(line[5]), line[9]))
                elif queryIndex == 1:
                    session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[1], line[4], line[9]))
                elif queryIndex == 2:
                    session.execute(query, (line[9], int(line[10]), line[1], line[4]))
                    
insert_table()

def select():
                
    song_length_session = "SELECT artist, song, length FROM song_length_session WHERE sessionId=338 AND itemInSession=4"
    song_playlist_session = "SELECT artist, song, firstName, lastName FROM song_playlist_session WHERE userId=10 AND sessionId=182"
    user_song = "SELECT firstName, lastName FROM user_song WHERE song='All Hands Against His Own'"

    try:
        rows1 = session.execute(song_length_session)
        rows2 = session.execute(song_playlist_session)
        rows3 = session.execute(user_song)
    except Exception as e:
        print(e)

    # Query 1:  Give me the artist, song title and song's length in the music app history that was heard during
    # sessionId = 338, and itemInSession = 4
    print ("Results from [song_length_session]")
    song_length_session_table = PrettyTable()
    
    song_length_session_table.field_names = ["Artist", "Song", "Length"]
    for row in rows1:
        song_length_session_table.add_row([row.artist, row.song, row.length])
    
    print(song_length_session_table)

    # Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)
    # for userid = 10, sessionid = 182
    print ("Results from [song_playlist_session]")
    song_playlist_session_table = PrettyTable()
    
    song_playlist_session_table.field_names = ["Artist", "Song", "First Name"]
    for row in rows2:
        song_playlist_session_table.add_row([row.artist, row.song, row.firstname])
    
    print(song_playlist_session_table)

    # Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'   
    print ("Results from [user_song]")
    user_song_table = PrettyTable()
    
    user_song_table.field_names = ["First Name", "Last Name"]
    for row in rows3:
        user_song_table.add_row([row.firstname, row.lastname])

    print(user_song_table)


select()

# Drop Tables
def drop_tables(session):    
    drop_statement = "drop table "
    tables = ["song_length_session", "song_playlist_session", "user_song"]
    for table in tables:
        try:
            session.execute(drop_statement +  table)
        except Exception as e:
            print(e)

drop_tables(session)
        
# Close the session and cluster connection¶
session.shutdown()
cluster.shutdown()
