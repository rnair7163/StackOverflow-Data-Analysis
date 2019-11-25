-- spam post 
# Loading the votes data where each post has a tag whether 
loaded_votes = LOAD 'votes_data.csv' Using PigStorage(',') AS (id:int,post_id:int, vote_type_id:int);
filtered_votes = FILTER loaded_votes BY vote_type_id ==12;

loaded_comments = LOAD 'data_comments.csv' Using PigStorage(',') AS (id:int, creation_date:chararray,post_id:int,user_id:int ,score:float);
filtered_comments = FILTER loaded_comments BY user_id!=0;


join_relation = JOIN filtered_votes BY post_id, filtered_comments BY id;
spammers = FOREACH join_relation GENERATE filtered_votes::post_id, filtered_comments::user_id;
STORE gener INTO '/pigresults/topSpammers';
