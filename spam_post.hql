-- spam post
select A.post_id, B.user_id from project.votes A , project.comments B
# Joining the postId of votes table with id of comments table and checking whether the voteTypeId from votes is 12 (which is confirming post has been voted spam) and usersId is not equal to 0 
where A.post_id=B.id AND (A.vote_type_id==12 AND B.user_id!=0);
