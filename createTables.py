import mysql.connector as db

# Create connection
cnx = db.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root"
)

db_curr = cnx.cursor()

# Create database if it doesn't exist
db_curr.execute("CREATE DATABASE IF NOT EXISTS cricket_dataset_practice")

# Connect to the database
cnx = db.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root",
    database="cricket_dataset_practice",
    autocommit=True
)

db_curr = cnx.cursor()

db_curr.execute("""CREATE TABLE registry (
    person_id VARCHAR(255) PRIMARY KEY,
    person_name VARCHAR(255)
);""")
#db_curr.execute("""DESC registry;""")
#print([i[0] for i in db_curr.fetchall()],"\n","registry created")
print("registry created")

#create match_details table
db_curr.execute("""CREATE TABLE match_details (
    match_id INT PRIMARY KEY,
    city VARCHAR(255),
    gender VARCHAR(255),
    match_type VARCHAR(255),
    match_type_number INT,
    overs INT,
    season VARCHAR(10),
    team_type VARCHAR(255),
    venue VARCHAR(255),
    team1 VARCHAR(255),
    team2 VARCHAR(255),
    toss_winner VARCHAR(255),
    toss_decision VARCHAR(255),
    winner VARCHAR(255),
    outcome_type VARCHAR(255),
    outcome_value VARCHAR(255),
    player_of_match VARCHAR(255),  -- Can be NULL
    balls_per_over INT,
    FOREIGN KEY (player_of_match) REFERENCES registry(person_id)
    ON UPDATE CASCADE  -- Update match_details if person_id changes
    ON DELETE SET NULL -- Set player_of_match to NULL if person is deleted
);""")
#db_curr.execute("""DESC match_details;""")
#print([i[0] for i in db_curr.fetchall()],"\n","match_details created!")
print("match_details created")


db_curr.execute("""CREATE TABLE officials (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id INT,
    person_id VARCHAR(255),
    official_type VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES match_details(match_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,  -- Delete officials if the match is deleted
    FOREIGN KEY (person_id) REFERENCES registry(person_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL -- Set person_id to NULL if person is deleted
);""")
#db_curr.execute("""DESC officials;""")
#print([i[0] for i in db_curr.fetchall()],"\n","officials created!")
print("officials created")

db_curr.execute("""CREATE TABLE players (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id INT,
    person_id VARCHAR(255),
    team_name VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES match_details(match_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,  -- Delete players if the match is deleted
    FOREIGN KEY (person_id) REFERENCES registry(person_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL -- Set person_id to NULL if person is deleted
);""")
#db_curr.execute("""DESC players;""")
#print([i[0] for i in db_curr.fetchall()],"\n","players created!")
print("players created")

db_curr.execute("""CREATE TABLE deliveries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id INT,
    innings INT,
    team VARCHAR(255),
    overs INT,
    balls INT,
    batter VARCHAR(255),
    bowler VARCHAR(255),
    non_striker VARCHAR(255),
    runs_batter INT,
    runs_extras INT,
    runs_total INT,
    powerplayed VARCHAR(255),
    powerplayed_type VARCHAR(255),
    player_out VARCHAR(255),
    dismissal_kind VARCHAR(255),
    fielders_involved VARCHAR(255),
    FOREIGN KEY (match_id) REFERENCES match_details(match_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,  -- Delete deliveries if the match is deleted
    FOREIGN KEY (batter) REFERENCES registry(person_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,  -- Set batter to NULL if person is deleted
    FOREIGN KEY (bowler) REFERENCES registry(person_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,  -- Set bowler to NULL if person is deleted
    FOREIGN KEY (non_striker) REFERENCES registry(person_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,  -- Set non_striker to NULL if person is deleted
    FOREIGN KEY (player_out) REFERENCES registry(person_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL   -- Set player_out to NULL if person is deleted
);""")
#db_curr.execute("""DESC deliveries;""")
#print([i[0] for i in db_curr.fetchall()],"\n","deliveries created!")
print("deliveries created")

db_curr.close()
cnx.close()
print("connection closed successfully")