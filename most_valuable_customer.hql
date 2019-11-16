SELECT A.DisplayName FROM
(SELECT * FROM sample.users ORDER BY reputation desc LIMIT 5) A;
