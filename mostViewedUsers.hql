-- mostViewedUsers
# Selecting displayname and views from users table and ordering the result by number of views in descending order and showing top 100 viewed users.
select A.displayname, A.views from project.users A
order by views desc limit 100;
