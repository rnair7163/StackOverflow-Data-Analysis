# computing the total number of answers given by the user.
create database user_answers;
create table if not exists user_answers.post_answers(id int, owner_user_id int, parent_id string, post_type_id int, score int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
;

LOAD DATA LOCAL INPATH 'post_answers_file_1.csv'
OVERWRITE INTO TABLE user_answers.post_answers;

create table total_answers as 
SELECT owner_user_id, count(distinct id) as total_answers FROM user_answers.post_answers group by owner_user_id;


# post questions
create table if not exists user_answers.post_questions(id int, accepted_answer_id int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'post_questions_file_1.csv'
OVERWRITE INTO TABLE user_answers.post_questions;

# computing the total number correct answers given by the user.

create table total_accepted_answers as
SELECT A.owner_user_id as owner_user_id, count(distinct A.id) as total_accepted_answers FROM user_answers.post_answers A,
user_answers.post_questions B
where A.id=B.accepted_answer_id
group by A.owner_user_id;

# 
select A.owner_user_id, B.total_accepted_answers, A.total_answers, float(B.total_accepted_answers/A.total_answers)*100  
from total_answers A, total_accepted_answers B 
where A.owner_user_id = B.owner_user_id
LIMIT 10;
