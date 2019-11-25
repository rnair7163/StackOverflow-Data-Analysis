
# Big data project
create database project;
create table if not exists project.user_data(id int, displayName string, creation_date string, last_access_date string, reputation int,up_votes int,down_votes int,views int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

create table if not exists project.post_answers(id int, owner_user_id int, parent_id string, post_type_id int, score int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
;

LOAD DATA LOCAL INPATH 'post_answers_file_1.csv'
OVERWRITE INTO TABLE project.post_answers;

create table project.total_answers as 
SELECT owner_user_id, count(distinct id) as total_answers FROM user_answers.post_answers group by owner_user_id;


# post questions
create table if not exists project.post_questions(id int, accepted_answer_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

