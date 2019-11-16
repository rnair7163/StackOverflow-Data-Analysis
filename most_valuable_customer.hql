SELECT A.DisplayName, A.Reputation FROM
(SELECT * FROM sample.users ORDER BY reputation desc LIMIT 100) A;
