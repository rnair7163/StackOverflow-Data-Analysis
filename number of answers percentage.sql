
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
