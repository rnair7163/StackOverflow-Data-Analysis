Select A.DisplayName from
(select * from MyProject.valuable_users order by reputation desc limit 5) A;