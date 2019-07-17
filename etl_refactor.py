# DROP TABLES
def drop_tables(db_session):    
    drop_statement = "drop table "
    tables = ["query1", "query2", "query3"]
    for table in tables:
        try:
            db_session.execute(drop_statement +  table)
        except Exception as e:
            print(e)    
        

# CREATE TABLES


def create_tables():
    create_statement = "CREATE TABLE IF NOT EXISTS "
    fields = "(artist text, firstName text, gender text, itemInSession int, lastName text, length double, level text, location text, sessionId int, song text, userId int, "
    tables = ["query1", "query2", "query3"]
    primary_keys = {
        "query1": "PRIMARY KEY (sessionId, itemInSession))",
        "query2": "PRIMARY KEY (userId, sessionId, itemInSession))",
        "query3": "PRIMARY KEY (song))"        
    }
    
    for table in tables:
        query = create_statement + table + fields + primary_keys.get(table)                 
        
        try:
            session.execute(query)
        except Exception as e:
            print(e)
            
        
# We have provided part of the code to set up the CSV file.

def insert_table():
    file = 'event_datafile_new_from_etlpy.csv'
    tables = ["query1", "query2", "query3"]
    insert_statement = "INSERT INTO "
    value_statement = " VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    parameters_statement = "(artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, song, userId)"
    line_statement = "(line[0], line[1], line[2], int(line[3]), line[4], float(line[5]), line[6], line[7], int(line[8]), line[9], int(line[10]))"
            
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            for table in tables:
                query = insert_statement + table + parameters_statement + value_statement
                session.execute(query, line_statement)
                                
                



                
def select():
                
    query1 = "SELECT artist, song, length FROM query1 WHERE sessionId=338 AND itemInSession=4"
    query2 = "SELECT artist, song, firstName, lastName FROM query2 WHERE userId=10 AND sessionId=182"
    query3 = "SELECT firstName, lastName FROM query3 WHERE song='All Hands Against His Own'"

    try:
        rows1 = session.execute(query1)
        rows2 = session.execute(query2)
        rows3 = session.execute(query3)
    except Exception as e:
        print(e)

    # Query 1:  Give me the artist, song title and song's length in the music app history that was heard during
    # sessionId = 338, and itemInSession = 4
    print ("QUERY 1: ")   
    for row in rows1:    
        print (row.artist, row.song, row.length)

    # Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)
    # for userid = 10, sessionid = 182
    print ("QUERY 2: ")
    for row in rows2:
        print (row.artist, row.song, row.firstname)

    # Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'   
    print ("QUERY 3: ")
    for row in rows3:
        print (row.firstname, row.lastname)


select()