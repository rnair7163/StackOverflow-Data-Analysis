
----- expert_users
 1) to measure this parameter we will compute the accepted answers to the number of answers answered by the user.

# computing the total number of answers given by the user.
SELECT owner_user_id, count(distinct id) FROM `bigquery-public-data.stackoverflow.posts_answers` group by owner_user_id;

# computing the total number correct answers given by the user.
SELECT A.owner_user_id, count(distinct A.id) FROM `bigquery-public-data.stackoverflow.posts_answers` A,
`bigquery-public-data.stackoverflow.posts_questions` B
where A.id=B.accepted_answer_id
and A.owner_user_id in (469335,
180524,
7148391,
654031)
group by A.owner_user_id;

file = LOAD 'post_answers.csv' Using PigStorage(',') AS (ID:int,title:chararray,body:chararray,
accepted_answer_id:chararray,
answer_count:chararray,
comment_count:int,
community_owned_date:int,
creation_date:chararray
favorite_count:chararray
last_activity_date:chararray
last_edit_date:chararray,
last_editor_display_name:chararray
last_editor_user_id:int,
owner_display_name:chararray,
owner_user_id:int,
parent_id:int,
post_type_id:int,
score:int,
tags:chararray,
view_count:chararray
);

grpd  = group file by owner_user_id;
cnt   = foreach grpd generate group, COUNT(distinct id);
		
