-- mostViewedUsers
select A.displayname, A.views from project.users A
order by views desc limit 100;
