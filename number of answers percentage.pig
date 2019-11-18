
# the purpose of this analysis is to identify the expert users community among the stack overflow users.

 ygude@hawk.iit.edu
 e643eb21d8A#

bdttestyg 
testyg@12345

# pig
hadoop fs -copyFromLocal /home/maria_dev/post_answers.csv /user/maria_dev

# 1) load function for the post_answers
post_answers = LOAD 'post_answers.csv' Using PigStorage(',') AS (ID:int,title:chararray,body:chararray,
accepted_answer_id:chararray,
answer_count:chararray,
comment_count:int,
community_owned_date:chararray,
creation_date:chararray,
favorite_count:chararray,
last_activity_date:chararray,
last_edit_date:chararray,
last_editor_display_name:chararray,
last_editor_user_id:int,
owner_display_name:chararray,
owner_user_id:int,
parent_id:int,
post_type_id:int,
score:int,
tags:chararray,
view_count:chararray
);

grpd_post_answers  = group post_answers by owner_user_id;
cnt_post_answers   = foreach grpd_post_answers generate group, COUNT(distinct grpd_post_answers::id);
		

post_questions = LOAD 'post_questions.csv' Using PigStorage(',') AS (id:int,
title:chararray,
body:chararray,
accepted_answer_id:int,
answer_count:int,
comment_count:int,
community_owned_date:chararray,
creation_date:chararray,
favorite_count:int,
last_activity_date:chararray,
last_edit_date:chararray,
last_editor_display_name:chararray,
last_editor_user_id:int,
owner_display_name:chararray,
owner_user_id:int,
parent_id:chararray,
post_type_id:int,
score:int,
tags:chararray,
view_count:int
);

grpd_post_questions  = group post_questions by owner_user_id;
cnt_post_questions   = foreach grpd_post_questions generate group, COUNT(distinct id);

 
 
 # combining  
join_accepted_answers=join post_answers by id, post_questions by accepted_answer_id;
grpd_accepted_answers  = group join_accepted_answers by post_answers::owner_user_id;
cnt_accepted_answers   = foreach grpd_accepted_answers generate group, COUNT(distinct post_answers::id);
 
 
# obtaining the percentage
join_percent_answers=join  cnt_accepted_answers by post_answers::owner_user_id, cnt_post_answers by owner_user_id
output=foreach join_percent_answers generate $cnt_accepted_answers::post_answers::owner_user_id,$post_answers::owner_user_id
dump join_percent_answers
