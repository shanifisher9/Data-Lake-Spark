### Data Lake

Sparkify is constantly growing and getting more data. Data can come in many forms ex: structured, semi-structures and not structured. A Data Lake can handle all of these types of data and can read many data formats but a data warehouse can only handle structured and semi-structured data. A Data Lake is very good for throwing all your data in one place and then being able to retrieve and analyse it. Many companies prefer a Data Lake because all their data can be in one location and you can create a Data Analysis on the fly. Anyone can use the data ex: Data Scientist, Business Intelligence, and Machine Learning Engineers while a Data Warehouse is typically used just by Business Analyst. In a Data Lake you can create Machine Learning, Graph Analytics and data exploration where as a Data Warehouse is generally used for reports and BI Visualizations


In this project I have created a Data Lake for Sparkify. This Data Lake works with AWS, S3, Hadoop and EC2(machines in the cloud). I have written the code using pySpark which is Spark with python code/syntax. Therefore, the data can run in your regular Jupyter notebook. After I read the data from the Data Lake and transformed the data using Spark and Hadoop, I saved the new tables in their own files on another S3 bucket. The process does take a long time because Sparkify has a lot of data but this would happen with whatever way your process the data. You could look at the Spark Web UI to see your clusters and look at the code. This code doesn't keep anything in the cloud, it just uses the machines to process the data and then ends the job.

# Data
The data is in 2 folders in S3 bucket udacity-dend. The first folder is called song_data and the second is called log-data. In each of these files there are more folders and then there is the data. The data is stored in json format in many different files. 

The song_data has information about all artist/songs on your app.
The log-data has information about all your users and what they have listened to and if they are a free member or not.

These datasets I transformed into 5 different tables that are in a star-schema database design.

# Tables

The data is devided into 5 tables in a star-schema database design. This is used becuase it is the easiest way to understand and look at the data. The star schema has a fact table and many dimension tables. The fact table has the core information about the songs played and the dimension tables has more information about what happened. The fact table can join and link to the dimension table to help you get all the data that you need.

1. SongPlays - This is the information about the songs played on Sparkify. This is the fact table.
    Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

2. Songs - This is the list of all songs on Sparkify. This is a dimension table.
    Columns: song_id, title, artist_id, year, duration

3. Users - This is information about all your users on Sparkify. This is a dimension table.
    Columns: user_id, first_name, last_name, gender, level
    
4. Artists - This is information about the artists who's songs are on Sparkify. This is a dimension table.
    Columns: artist_id, name, location, latitude, longitude

5. TimeStamp - This is a list of times that were generated from Sparkify's timestamp column. This is a dimension table.
    Columns: start_time, hour, day, week, month, year, weekday

Using these Tables you can generate a report to get to know a lot about your data at Sparkify. In all of these tables your first column is your primary key. The primary key allows you to join to the fact table and get correct and accurate information. It is best to join using these keys.

# Running the Scripts

To run the scripts you will need to create an IAM role in AWS. This role has to have full access to S3 so you could read and write the data from S3. Once you create the role you will need the secret key and password to put into the dl.cfg file. This is the only way for you to have access to the file.

Another tip is to create your own S3 bucket where you would like to store these tables/data. I created one on my AWS account and you can use it but if you want to store it somewhere else you can. You need to go to etl.py and at the bottom there is a variable called output_data. You should change the path to your S3 bucket instead of sf-dl-project.

etl.py - Once everything is created you can run this script. This will run the ETL and convert your data to the tables mentioned about. To run this script just open a new editor, highlight all the code and run the scripts.

After all of this is done your data will be ready to analysed. 

