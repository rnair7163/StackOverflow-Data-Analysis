
# Big data project
create database project;

# creating users table
create table if not exists project.user_data(id int, displayName string, creation_date string, last_access_date string, reputation int,up_votes int,down_votes int,views int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

# Load users dataset 
LOAD DATA LOCAL INPATH 'user.csv'
OVERWRITE INTO TABLE project.user_data;

# Creating Post_answers table
create table if not exists project.post_answers(id int, owner_user_id int, parent_id string, post_type_id int, score int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
;

# Loading Post_answers
LOAD DATA LOCAL INPATH 'post_answers_file_1.csv'
OVERWRITE INTO TABLE project.post_answers;

# post questions
create table if not exists project.post_questions(id int, accepted_answer_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

