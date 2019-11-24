
# the purpose of this analysis is to identify the expert users community among the stack overflow users.

 ygude@hawk.iit.edu
 e643eb21d8A#

bdttestyg 
testyg@12345

# pig
hadoop fs -copyFromLocal /home/maria_dev/post_answers.csv /user/maria_dev

# 1) load function for the post_answers
post_answers_file = LOAD 'post_answers_file_1.csv' Using PigStorage(',') AS (id:int,
owner_user_id:int,
parent_id:chararray,
post_type_id:int,
score:int);
grpd_post_answers  = group post_answers_file by owner_user_id;
cnt_post_answers   = foreach grpd_post_answers generate group, COUNT(post_answers_file.id);

post_questions_file = LOAD 'post_questions_file_1.csv' Using PigStorage(',') AS (id:int,
accepted_answer_id:int);

join_accepted_answers = JOIN post_answers_file BY id, post_questions_file BY accepted_answer_id;
grpd_accepted_answers  = group join_accepted_answers by owner_user_id;
cnt_accepted_answers   = foreach grpd_accepted_answers generate group, COUNT(join_accepted_answers.post_answers_file::id);
 
cnt_post_answers_1 = foreach grpd_post_answers generate group as owner_user_id, COUNT(post_answers_file.id) as post_ans;
cnt_accepted_answers_1 = foreach grpd_accepted_answers generate group as owner_user_id, COUNT(join_accepted_answers.post_answers_file::id) as post_acc_ans;
 
 
# obtaining the percentage
join_percent_answers= join cnt_accepted_answers_1 by owner_user_id, cnt_post_answers_1 by owner_user_id;

output = foreach join_percent_answers generate cnt_accepted_answers_1::owner_user_id;
dump join_percent_answers;
