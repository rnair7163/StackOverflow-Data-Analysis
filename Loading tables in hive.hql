
# Big data project
create database project;
create table if not exists project.user_data(id int, displayName string, creation_date string, last_access_date string, reputation int,up_votes int,down_votes int,views int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

