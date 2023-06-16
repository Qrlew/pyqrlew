-- SELECT COUNT(*) AS count_all FROM census;
SELECT COUNT(age) AS count_all FROM census;
SELECT SUM(education_num) AS sum_education_num FROM census;
SELECT AVG(education_num) AS my_avg FROM census;
SELECT STDDEV(education_num) AS my_std FROM census;
SELECT VARIANCE(education_num) AS my_var FROM census;
SELECT SUM(education_num * age + 4 + LOG(age + 2)) AS my_sum FROM census;
-- SELECT age, sex, SUM(education_num) AS sum_education_num FROM census GROUP BY age, sex ORDER BY age DESC; -- Problem with the order by
-- SELECT COUNT(*) AS count_all, SUM(education_num) AS sum_education_num, AVG(education_num) AS avg_education_num, VARIANCE(education_num) AS var_education_num, STDDEV(education_num) AS stddev_education_num FROM census; -- COUNT(*)
SELECT SUM(education_num * age + 4 + LOG(age + 3)) AS my_sum FROM census;
SELECT VARIANCE(education_num) AS my_var, marital_status FROM census GROUP BY marital_status;
SELECT (VARIANCE(education_num)) AS my_var, marital_status FROM census GROUP BY marital_status;
-- SELECT sex, SUM(capital_gain / 10000) AS my_sum, SUM(education_num) AS sum_education_num FROM census GROUP BY sex HAVING NOT ( sex = 'Female' ) ORDER BY my_sum DESC LIMIT 10; -- HAVING not supported
-- SELECT marital_status, SUM(capital_gain / 100000) AS my_sum, SUM(education_num) AS sum_education_num FROM census GROUP BY marital_status HAVING marital_status = 'Divorced'; -- HAVING not supported
SELECT POWER(AVG(age), 2) AS my_res FROM census;
SELECT 1 + AVG(age) AS my_res FROM census;
SELECT SUM(age)  AS my_res FROM census as p;
-- SELECT COUNT(*) FROM census WHERE workclass LIKE 'Married%' -- COUNT(*)
SELECT SUM(education_num) AS my_sum FROM census GROUP BY LOG(CASE WHEN age < 50 THEN 50 ELSE 1 END);
SELECT COUNT(education_num) As my_sum FROM census GROUP BY CASE WHEN age < 50 THEN 50 ELSE 1 END;
-- SELECT CASE WHEN age < 50 THEN 50 ELSE 1 END, COUNT(education_num) AS my_sum FROM census GROUP BY CASE WHEN age < 50 THEN 50 ELSE 1 END;
-- SELECT age, SUM(education_num) AS my_sum, SUM(education_num) AS sum_education_num FROM census GROUP BY age HAVING SUM(education_num) > 1 ORDER BY age; -- HAVING
-- SELECT age AS age1, SUM(education_num) FROM census GROUP BY age1 ORDER BY age1;
SELECT SUM(age) AS my_sum FROM census GROUP BY marital_status, CASE WHEN age > 90 THEN 1 ELSE 0 END ORDER BY my_sum;
-- SELECT CASE WHEN age > 90 THEN 1 ELSE 0 END, SUM(age) AS my_sum FROM census GROUP BY marital_status, CASE WHEN age > 90 THEN 1 ELSE 0 END ORDER BY my_sum;
-- SELECT CASE WHEN age > 90 THEN 1 ELSE 0 END AS my_col, SUM(age) AS my_sum FROM census GROUP BY my_col ORDER BY my_sum;
SELECT SUM(CASE WHEN age > 90 THEN 1 ELSE 0 END) AS s1, SUM(age) AS s2 FROM census;
SELECT ( 2 * (SUM(CASE WHEN age > 90 THEN 1 ELSE 0 END))) AS s1 FROM census;
-- SELECT capital_gain, COUNT(*) FROM census GROUP BY capital_gain;
-- SELECT capital_gain, COUNT(1) FROM census GROUP BY capital_gain;
SELECT capital_gain, COUNT(age) AS count_all FROM census GROUP BY capital_gain;
-- SELECT EXTRACT( YEAR FROM "検知日時" ) AS _year, COUNT(*) FROM beacon GROUP BY EXTRACT(YEAR FROM "検知日時");
-- SELECT age AS "my age", 3 * COS(COUNT(*)) AS my_ln_count, AVG(education_num) AS "my avg" FROM census GROUP BY age ORDER BY age, "my avg";
-- SELECT age AS "my age", 3 * COS(COUNT(*)) AS my_ln_count, AVG(education_num) AS "my avg" FROM census GROUP BY "my age" ORDER BY "my age", "my avg";
-- SELECT age, COUNT(*) FROM census WHERE age BETWEEN 18 AND 30 GROUP BY age;
-- SELECT age, COUNT(*) FROM census WHERE age NOT BETWEEN 18 AND 30 GROUP BY age;
-- SELECT COUNT(*) FROM census WHERE native_country IN ('Holand-Netherlands', 'Cuba', 'Italy', 'England');
-- SELECT native_country, COUNT(*) FROM census WHERE native_country NOT IN ('Holand-Netherlands', 'Cuba', 'Italy', 'England') GROUP BY native_country;
-- SELECT CAST(age AS VARCHAR), COUNT(*) FROM census GROUP BY age;
-- SELECT UPPER(marital_status), COUNT(*) FROM census GROUP BY marital_status;
-- SELECT LOWER(marital_status), COUNT(*) FROM census GROUP BY marital_status;
-- SELECT CONCAT(age, marital_status), COUNT(*) FROM census GROUP BY age, marital_status;
-- SELECT COALESCE(age, 1) AS new_age, COUNT(*) FROM census GROUP BY COALESCE(age, 1) ORDER BY new_age;
-- SELECT TRIM(education) AS new_income, COUNT(*) FROM census GROUP BY TRIM(education);
-- SELECT SUBSTRING(education FROM 1 FOR 4), COUNT(*) FROM census GROUP BY education;
-- SELECT POSITION('m' in education), COUNT(*) FROM census GROUP BY education;
-- SELECT CHAR_LENGTH(education), COUNT(*) FROM census GROUP BY education;
-- SELECT SUM(census.age) AS s1, SUM(age) AS s2 FROM census;
SELECT table1.sex AS c1, SUM(table1.age) AS c2, SUM(age) AS c2 FROM census AS table1 GROUP BY table1.sex;
-- SELECT COUNT(*) FROM census WHERE marital_status LIKE 'W%';
-- SELECT age AS age1, SUM(education_num) AS s1 FROM census WHERE age IS NOT NULL GROUP BY age;
-- SELECT SUM("マップのY座標") FROM beacon; -- Does not handle '"'"
-- SELECT "所属部署", COUNT("マップのY座標") FROM beacon GROUP BY "所属部署";
-- SELECT 2 * SUM (3 * LOG("マップのY座標" + 5)) FROM beacon GROUP BY 3 * "マップのY座標";
-- SELECT "所属部署" AS "MY_COL", COUNT(*) FROM beacon GROUP BY "所属部署";
-- SELECT "所属部署" AS "MY_COL", COUNT(*) FROM beacon GROUP BY "MY_COL";
-- SELECT CURRENT_TIMESTAMP - "検知日時", COUNT(*) FROM beacon GROUP BY "検知日時";
-- SELECT COUNT("table_alias"."age") AS "count_alias"  FROM census AS "table_alias";
-- SELECT age, 3 * COUNT(*), CASE WHEN COUNT(*) > 10 THEN 'large' ELSE 'small' END, COUNT(*) FROM census GROUP BY age;
-- SELECT age, COUNT(*) FROM census WHERE age IN (20, 30, 40, 50) GROUP BY age;
-- SELECT age, CASE WHEN age IN (20, 30, 40, 50) THEN 'decade' ELSE '-' END, COUNT(*) FROM census GROUP BY age;
-- SELECT age, CASE WHEN age BETWEEN 10 AND 40 THEN 0 ELSE 1 END, COUNT(*) FROM census GROUP BY age;
-- SELECT age, CASE WHEN age IS Null THEN 'Null' ELSE 'NotNull' END, COUNT(*) FROM census GROUP BY age;
-- SELECT age, CASE WHEN COUNT(*) BETWEEN 0 AND 10 THEN 0 ELSE 1 END, COUNT(*) FROM census GROUP BY age;
-- SELECT COUNT(*) FROM beacon WHERE "検知日時" BETWEEN '1900-01-01' AND '2025-01-01';
-- SELECT CASE WHEN SUM(capital_gain) < 10 THEN SUM(capital_gain) ELSE SUM(capital_gain) END FROM census;
-- SELECT CASE WHEN SUM(age) > 90 THEN 90 WHEN SUM(age) < 10 THEN 10 ELSE 0 END FROM census;
-- SELECT CASE WHEN SUM(age) > 90 THEN 90 ELSE SUM(age) END FROM census;
-- SELECT SUM("age"), STDDEV("age"), VARIANCE("age") FROM census;
-- SELECT SUM("Code Client Patient"), AVG("Code Client Patient"), STDDEV("Code Client Patient") FROM "suivi-patients";
-- SELECT COUNT(*) AS count_all FROM census LIMIT 30 OFFSET 5;
-- SELECT COUNT(*) FROM census WHERE marital_status ILIKE 'W%';
-- SELECT EXTRACT( HOUR FROM "検知日時" ), COUNT(*) FROM beacon GROUP BY "検知日時";
-- SELECT SUM(capital_gain) FROM census GROUP BY capital_gain;
-- SELECT SUM(100 * age) FROM census WHERE age = 40
-- SELECT SUM(-100 * age) FROM census WHERE age = 40