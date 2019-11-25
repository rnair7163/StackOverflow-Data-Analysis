-- spam post
# Selecting postId and userId from votes table 
select A.postId, A.userId from votes A , comments B

# Joining the postId of votes table with id of comments table and checking whether the voteTypeId from votes is 12 (which is confirming post has been voted spam) and usersId is not equal to 0 
where A.postId=B.id AND (A.voteTypeId==12 AND B.usersId!=0);
