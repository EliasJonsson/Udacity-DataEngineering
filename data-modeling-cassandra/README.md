## Data Modeling (Postgres)

### Project
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

Here is a creation of an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project.

### Data
The data is distributed in events file all over the event_data folder. And each file looks like this
<img src="images/image_event_datafile_new.png" alt="" width="30%"/>

### Queries

In the project we model for three types of queries.

1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'.

For that we create 3 different tables.

**Table 1**:
Columns: artist_name text, song text, length double, session_id int, item_in_session int
Primary Key: session_id, item_in_session

**Table 2**:
Columns: artist_name text, song text, user_first_name text, user_last_name text, user_id int, session_id int, item_in_session int
Primary Key: user_id, session_id, item_in_session
Clustering Key: item_in_session

**Table 3**:
Columns: artist_name text, song text, user_first_name text, user_last_name text, user_id int
Primary Key: song, user_id, artist_name

### How to Run

Run the Jupyter Notebook `Project_Cassandra`

### Repo structure
In this directory are the following files
1. `event_data` Folder with all the event data.
2. `images` Folder with images used for markdown and more.
3. `event_datafile_new.csv` a file generated from event data used and generated in `Project_Cassandra`.
4. `Project_Cassandra` a Jupyter Notebook that runs the project.
