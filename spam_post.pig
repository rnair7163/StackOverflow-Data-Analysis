-- spam post 
loaded = LOAD 'hdfs://Shrini:9000/stackexchange/Votes.txt' Using PigStorage() AS (rowId:int, postId:int, creationDate:chararray,voteTypeId:int, userId:int);
filtered = FILTER loaded BY voteTypeId ==12;
loadedtwo = LOAD 'hdfs://Shrini:9000/stackexchange/Comments.txt' Using PigStorage() AS (rowsId:int, usersId:int, creationDate:chararray,postIds:int,score:float);
filtering = FILTER loadedtwo BY usersId!=0;
joined = JOIN filtered BY postId, filtering BY rowsId;
gener = FOREACH joined GENERATE postId,usersId;
STORE gener INTO '/pigresults/topSpammers';