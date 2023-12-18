SELECT COUNT(*) AS count_all FROM census;
SELECT COUNT(age) AS count_all FROM census;
SELECT SUM(education_num) AS sum_education_num FROM census WHERE education_num > 0 AND education_num < 15;
SELECT AVG(education_num) AS my_avg FROM census WHERE education_num > 0 AND education_num < 15;
SELECT STDDEV(education_num) AS my_std FROM census WHERE education_num > 0 AND education_num < 15;
SELECT VARIANCE(education_num) AS my_var FROM census WHERE education_num > 0 AND education_num < 15;
SELECT SUM(education_num * age + 4 + LOG(age + 2)) AS my_sum FROM census WHERE education_num > 0 AND education_num < 15 AND age > 18 AND age < 90;
SELECT age, sex, SUM(education_num) AS sum_education_num FROM census WHERE education_num > 0 AND education_num < 15 AND age > 18 AND age < 90 GROUP BY age, sex ORDER BY age DESC;
SELECT COUNT(*) AS count_all, SUM(education_num) AS sum_education_num, AVG(education_num) AS avg_education_num, VARIANCE(education_num) AS var_education_num, STDDEV(education_num) AS stddev_education_num FROM census WHERE education_num > 0 AND education_num < 15;
SELECT SUM(education_num * age + 4 + LOG(age + 3)) AS my_sum FROM census WHERE education_num > 0 AND education_num < 15 AND age > 18 AND age < 90;
SELECT VARIANCE(education_num) AS my_var, marital_status FROM census WHERE education_num > 0 AND education_num < 15 GROUP BY marital_status;
SELECT (VARIANCE(education_num)) AS my_var, marital_status FROM census WHERE education_num > 0 AND education_num < 15 GROUP BY marital_status;
SELECT sex, SUM(capital_loss / 10000) AS my_sum, SUM(education_num) AS sum_education_num FROM census WHERE education_num > 0 AND education_num < 15 GROUP BY sex HAVING NOT ( sex = 'Female' ) ORDER BY my_sum DESC LIMIT 10;
-- SELECT marital_status, SUM(capital_loss / 100000) AS my_sum, SUM(education_num) AS sum_education_num FROM census WHERE education_num > 0 AND education_num < 15 AND capital_loss > 2231 AND capital_loss < 4356 GROUP BY marital_status HAVING marital_status = 'Divorced'; --pyo3_runtime.PanicException: attempt to divide by zero
SELECT POWER(AVG(age), 2) AS my_res FROM census WHERE age >20 AND age < 90;
SELECT 1 + AVG(age) AS my_res FROM census WHERE age >20 AND age < 90;
SELECT SUM(age)  AS my_res FROM census as p WHERE age >20 AND age < 90;
SELECT COUNT(*) AS count_all FROM census WHERE workclass LIKE 'Married%'
--SELECT SUM(education_num) AS my_sum FROM census  WHERE age >20 AND age < 90 AND education_num > 0 AND education_num < 15 GROUP BY LOG(CASE WHEN age < 50 THEN 50 ELSE 1 END); -- group by exprs
--SELECT COUNT(education_num) As my_sum FROM census  WHERE age >20 AND age < 90 AND education_num > 0 AND education_num < 15 GROUP BY CASE WHEN age < 50 THEN 50 ELSE 1 END; -- group by exprs
--SELECT CASE WHEN age < 50 THEN 50 ELSE 1 END AS case_age, COUNT(education_num) AS my_sum FROM census GROUP BY CASE WHEN age < 50 THEN 50 ELSE 1 END;-- group by exprs
SELECT age, SUM(education_num) AS my_sum, SUM(education_num) AS sum_education_num FROM census WHERE age >20 AND age < 90 AND education_num > 0 AND education_num < 15 GROUP BY age HAVING SUM(education_num) > 1 ORDER BY age;
--SELECT age AS age1, SUM(education_num) FROM census GROUP BY age1 ORDER BY age1;
-- SELECT SUM(age) AS my_sum FROM census WHERE age >20 AND age < 90 AND education_num > 0 AND education_num < 15 GROUP BY marital_status, CASE WHEN age > 90 THEN 1 ELSE 0 END;  -- group by exprs
-- SELECT SUM(age) AS my_sum FROM census  WHERE age >20 AND age < 90 AND education_num > 0 AND education_num < 15 GROUP BY marital_status, CASE WHEN age > 90 THEN 1 ELSE 0 END ORDER BY my_sum;  -- group by exprs
--SELECT CASE WHEN age > 80 THEN 1 ELSE 0 END, SUM(age) AS my_sum FROM census GROUP BY marital_status, CASE WHEN age > 80 THEN 1 ELSE 0 END ORDER BY my_sum;-- group by exprs
--SELECT CASE WHEN age > 90 THEN 1 ELSE 0 END AS my_col, SUM(age) AS my_sum FROM census GROUP BY my_col ORDER BY my_sum;-- group by exprs
SELECT SUM(CASE WHEN age > 80 THEN 1 ELSE 0 END) AS s1, SUM(age) AS s2 FROM census WHERE age >20 AND age < 90;
SELECT ( 2 * (SUM(CASE WHEN age > 90 THEN 1 ELSE 0 END))) AS s1 FROM census WHERE age >20 AND age < 90;
SELECT capital_loss, COUNT(*) AS my_count FROM census GROUP BY capital_loss;
SELECT capital_loss, COUNT(1) AS my_count FROM census GROUP BY capital_loss;
SELECT capital_loss, COUNT(age) AS count_all FROM census GROUP BY capital_loss;
--SELECT EXTRACT( YEAR FROM "検知日時" ) AS _year, COUNT(*) AS my_count FROM beacon GROUP BY EXTRACT(YEAR FROM "検知日時");
--SELECT age AS "my age", 3 * COS(COUNT(*)) AS my_ln_count, AVG(education_num) AS "my avg" FROM census GROUP BY age ORDER BY age, "my avg";
--SELECT age AS "my age", 3 * COS(COUNT(*)) AS my_ln_count, AVG(education_num) AS "my avg" FROM census GROUP BY "my age" ORDER BY "my age", "my avg";
SELECT age, COUNT(*) AS my_count FROM census WHERE age BETWEEN 18 AND 30 GROUP BY age;
SELECT age, COUNT(*) AS my_count FROM census WHERE age NOT BETWEEN 18 AND 30 GROUP BY age;
SELECT COUNT(*) AS my_count FROM census WHERE native_country IN ('Holand-Netherlands', 'Cuba', 'Italy', 'England');
SELECT native_country, COUNT(*) AS my_count FROM census WHERE native_country NOT IN ('Holand-Netherlands', 'Cuba', 'Italy', 'England') GROUP BY native_country;
SELECT CAST(age AS VARCHAR) AS my_age, COUNT(*) AS my_count FROM census GROUP BY age;
SELECT UPPER(marital_status) AS m, COUNT(*) AS my_count FROM census GROUP BY marital_status;
SELECT LOWER(marital_status)AS m, COUNT(*) AS my_count FROM census GROUP BY marital_status;
SELECT CONCAT(age, marital_status) AS m , COUNT(*) AS my_count FROM census GROUP BY age, marital_status;
--SELECT COALESCE(age, 1) AS new_age, COUNT(*) AS my_count FROM census GROUP BY COALESCE(age, 1) ORDER BY new_age;
--SELECT TRIM(education) AS new_capital_loss, COUNT(*) AS my_count FROM census GROUP BY TRIM(education);
SELECT SUBSTRING(education FROM 1 FOR 4) AS m, COUNT(*) AS my_count FROM census GROUP BY education;
SELECT POSITION('m' in education) AS m , COUNT(*) AS my_count FROM census GROUP BY education;
SELECT CHAR_LENGTH(education) AS m, COUNT(*) AS my_count FROM census GROUP BY education;
SELECT SUM(census.age) AS s1, SUM(age) AS s2 FROM census WHERE age > 20 ANd age < 90;
SELECT table1.sex AS c1, SUM(table1.age) AS c2, SUM(age) AS c3 FROM census AS table1 WHERE age > 20 ANd age < 90 GROUP BY table1.sex;
SELECT COUNT(*) AS my_count FROM census WHERE marital_status LIKE 'W%';
SELECT age AS age1, SUM(education_num) AS s1 FROM census WHERE age IS NOT NULL AND education_num > 0 AND education_num < 15 GROUP BY age;
-- SELECT SUM("マップのY座標") FROM beacon; -- Does not handle '"'"
-- SELECT "所属部署", COUNT("マップのY座標") FROM beacon GROUP BY "所属部署";
-- SELECT 2 * SUM (3 * LOG("マップのY座標" + 5)) FROM beacon GROUP BY 3 * "マップのY座標";
-- SELECT "所属部署" AS "MY_COL", COUNT(*) FROM beacon GROUP BY "所属部署";
-- SELECT "所属部署" AS "MY_COL", COUNT(*) FROM beacon GROUP BY "MY_COL";
-- SELECT CURRENT_TIMESTAMP - "検知日時", COUNT(*) FROM beacon GROUP BY "検知日時";
SELECT COUNT("table_alias"."age") AS "count_alias"  FROM census AS "table_alias";
SELECT age, 3 * COUNT(*) AS c1, CASE WHEN COUNT(*) > 10 THEN 'large' ELSE 'small' END AS c2, COUNT(*) AS c3 FROM census GROUP BY age;
SELECT age, COUNT(*) AS c1 FROM census WHERE age IN (20, 30, 40, 50) GROUP BY age;
SELECT age, CASE WHEN age IN (20, 30, 40, 50) THEN 'decade' ELSE '-' END AS case_age, COUNT(*) AS c1 FROM census GROUP BY age;
SELECT age, CASE WHEN age BETWEEN 10 AND 40 THEN 0 ELSE 1 END AS case_age, COUNT(*) AS c1 FROM census GROUP BY age;
SELECT age, CASE WHEN age IS Null THEN 'Null' ELSE 'NotNull' END AS case_age, COUNT(*) AS c1 FROM census GROUP BY age;
SELECT age, CASE WHEN COUNT(*) BETWEEN 0 AND 10 THEN 0 ELSE 1 END AS case_age, COUNT(*) AS c1 FROM census GROUP BY age;
--SELECT COUNT(*) FROM beacon WHERE "検知日時" BETWEEN '1900-01-01' AND '2025-01-01';
SELECT CASE WHEN SUM(capital_loss) < 10 THEN SUM(capital_loss) ELSE SUM(capital_loss) END AS s1 FROM census WHERE capital_loss > 2231 AND capital_loss < 4356;
SELECT CASE WHEN SUM(age) > 90 THEN 90 WHEN SUM(age) < 10 THEN 10 ELSE 0 END AS s1 FROM census WHERE age > 20 AND age < 90;
SELECT CASE WHEN SUM(age) > 90 THEN 90 ELSE SUM(age) END AS s1 FROM census WHERE age > 20 AND age < 90;
--SELECT SUM("age"), STDDEV("age"), VARIANCE("age") FROM census;
--SELECT SUM("Code Client Patient"), AVG("Code Client Patient"), STDDEV("Code Client Patient") FROM "suivi-patients";
--SELECT age, COUNT(*) AS count_all FROM census GROUP BY age LIMIT 20 OFFSET 1
SELECT COUNT(*) AS c1 FROM census WHERE marital_status ILIKE 'W%';
--SELECT EXTRACT( HOUR FROM "検知日時" ), COUNT(*) FROM beacon GROUP BY "検知日時";
--SELECT SUM(capital_loss) AS s1 FROM census WHERE capital_loss > 2231 AND capital_loss < 4356 GROUP BY capital_loss; -- expr in group by and agg is the same
SELECT SUM(100 * age) AS s1 FROM census WHERE age = 40 WHERE age < 20 AND age < 90;;
SELECT SUM(-100 * age) AS s1 FROM census WHERE age = 40 WHERE age < 20 AND age < 90;
-- SQL unlimited queries
SELECT AVG(grouped_sum_age) AS res FROM (SELECT SUM(capital_loss) AS grouped_sum_age FROM census WHERE capital_loss > 2231 AND capital_loss < 4356 GROUP BY age) AS subquery;
SELECT AVG(grouped_sum_age) AS res FROM (SELECT SUM(table1.capital_loss) AS grouped_sum_age FROM census AS table1 WHERE capital_loss > 2231 AND capital_loss < 4356 GROUP BY age) AS subquery;
--SELECT SUM(log_capital_loss) AS res1, SUM(sqrt_capital_loss) AS res2, SUM(inv_capital_loss) AS res3 FROM (SELECT LOG(capital_loss + 1.) AS log_capital_loss, SQRT(capital_loss) AS sqrt_capital_loss, 1/(capital_loss+1) AS inv_capital_loss FROM census WHERE capital_loss < 40000) AS subquery;--rust: attempt to divide by zero
--SELECT COUNT(*) FROM census TABLESAMPLE BERNOULLI (1) REPEATABLE (200);
WITH my_table1 AS ( SELECT age, marital_status, COUNT(*) AS count_all FROM census GROUP BY age, marital_status ORDER BY age, marital_status ), my_table2 AS ( SELECT 2 * age AS two_age, marital_status, count_all FROM my_table1 ) SELECT two_age, COUNT(*)  AS res FROM my_table2 GROUP BY two_age LIMIT 15;
SELECT sex, COUNT(DISTINCT age) AS my_count, SUM(DISTINCT age) AS my_age, VARIANCE(DISTINCT age) AS my_var, STDDEV (DISTINCT age) AS my_stddev FROM census WHERE age > 20 AND age < 90 GROUP BY sex;
SELECT COUNT(DISTINCT age) AS my_count, SUM(DISTINCT age) AS my_age, VARIANCE(DISTINCT age) AS my_var, STDDEV (DISTINCT age) AS my_stddev FROM census WHERE age > 20 AND age < 90;
--SELECT SUM(s1) AS res1, SUM(s2)  AS res2 FROM (SELECT SUM(DISTINCT capital_loss) AS s1, SUM(DISTINCT age) AS s2 FROM census WHERE age > 20 AND age < 90 AND capital_loss > 2231 AND capital_loss < 4356 GROUP BY sex HAVING COUNT(*) > 5) AS sq; -- WITH query "join_1v8b" has 3 columns available but 4 columns specified
SELECT SUM(DISTINCT age) AS res FROM census WHERE age > 20 AND age < 90;
SELECT DISTINCT SUM(age) AS res FROM census WHERE age > 20 AND age < 90;
SELECT COUNT(DISTINCT age) AS res, SUM(DISTINCT age) AS res2, AVG(DISTINCT age) AS res3, VARIANCE(DISTINCT age) AS res4, STDDEV(DISTINCT age) AS res5 FROM census WHERE age > 20 AND age < 90;
--SELECT sex, COUNT(DISTINCT age) AS res1, AVG(DISTINCT age) AS res2, SUM(DISTINCT age) AS res3, AVG(DISTINCT age) AS res4, VARIANCE(DISTINCT age) AS res5, STDDEV(DISTINCT age) AS res6 FROM census WHERE age > 20 AND age < 90 GROUP BY sex; -- WITH query "join_e_jm" has 6 columns available but 8 columns specified
--SELECT sex, SUM(age) AS res, VARIANCE(age) AS res1, SUM(DISTINCT age) AS res2, VARIANCE(DISTINCT age) AS res3, SUM(DISTINCT capital_loss) AS res4, VARIANCE(DISTINCT capital_loss) AS res5 FROM census WHERE age > 20 AND age < 90 AND capital_loss > 2231 AND capital_loss < 4356 GROUP BY sex;
--SELECT SUM(age) AS res1, VARIANCE(age) AS res2, SUM(DISTINCT age) AS res3, VARIANCE(DISTINCT age) AS res4, SUM(DISTINCT capital_loss) AS res5, VARIANCE(DISTINCT capital_loss) AS res6 FROM census WHERE age > 20 AND age < 90 AND capital_loss > 2231 AND capital_loss < 4356 GROUP BY sex ;
SELECT SUM(DISTINCT age) AS my_sum FROM census WHERE sex = 'Female' AND age > 20 AND age < 90 HAVING COUNT(*) > 5;
--SELECT COUNT(*) AS res, SUM(DISTINCT age) AS my_sum FROM census WHERE age > 20 AND age < 90 GROUP BY CEIL(capital_loss / 100); --group by expr
--SELECT capital_loss, COUNT(*) AS res, SUM(DISTINCT age) AS my_sum FROM census WHERE age > 20 AND age < 90 GROUP BY capital_loss ORDER BY capital_loss, my_sum LIMIT 5;
WITH my_table1 AS (SELECT * FROM census), my_table2 AS (SELECT * FROM census) SELECT COUNT(*) AS res, SUM(DISTINCT my_table1.age) AS res1, SUM(DISTINCT my_table2.age) AS res2 FROM my_table1 JOIN my_table2 ON my_table1.fnlwgt = my_table2.fnlwgt WHERE my_table1.age > 20 AND my_table1.age < 90 AND my_table2.age > 20 AND my_table2.age < 90;