
# Load users dataset 
LOAD DATA LOCAL INPATH 'user.csv'
OVERWRITE INTO TABLE users.user_data;

# Selecting displayname and views from users table and ordering the result by number of views in descending order and showing top 100 viewed users.
select A.displayName, A.views from users.user_data A
order by views desc limit 100;
