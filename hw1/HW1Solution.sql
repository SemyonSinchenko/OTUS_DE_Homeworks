/* 1. Разведочный анализ */

CREATE table sinchenko.kiva_loans_clean as 
SELECT * FROM kiva_loans AS t WHERE  t.dt LIKE "201%";

/* */

SELECT
count(*) as count,
max(dt) as max_date,
min(dt) as min_date,
max(loan_amount) as max_amount,
min(loan_amount) as min_amount
FROM kiva_loans_clean;

/* count	max_date	min_date	max_amount	min_amount */
/* 670311	2017-07-26	2014-01-01	9975.0	100.0 */

SELECT count(*) FROM kiva_loans_clean
WHERE loan_amount IS NULL;

/* _c0 */
/* 0 */

SELECT unix_timestamp(dt, "yyyy-MM-dd") FROM kiva_loans_clean;

/* _c0 */
/* 1388534400 */
/* 1388534400 */
/* ... */

/* 2. Пользователи Kiva */

SELECT region, count(*) as cnt FROM kiva_loans_clean
GROUP BY region
ORDER BY cnt DESC;

/** 
 	region	cnt
1		56579
2	Kaduna	9999
3	Lahore	7178
4	Rawalpindi	4496
5	Cusco	3812
6	Dar es Salaam	3719
7	Kisii	3546
8	Palo, Leyte	3320
9	Narra, Palawan	3197
10	Quezon, Palawan	3120
11	Kitale	3104
12	Thanh Hoá	3082
**/

SELECT t2.world_region, count(*) as cnt
FROM kiva_loans_clean as t1
INNER JOIN kiva_mpi_region_locations as t2
on t1.region = t2.region
GROUP BY t2.world_region
ORDER BY cnt DESC;

/** 
 	t2.world_region	cnt
1		99805356
2	Europe and Central Asia	680347
3	Arab States	227052
4	Latin America and Caribbean	199047
5	East Asia and the Pacific	125544
6	South Asia	113182
7	Sub-Saharan Africa	72749
**/

CREATE TABLE sinchenko.genders as 
SELECT 
id, 
country,
region,
loan_amount,
CASE
WHEN borrower_genders like "female%" THEN "female"
WHEN borrower_genders like "male%" THEN "male"
ELSE "other" END as pure_gender 
from sinchenko.kiva_loans_clean;

SELECT
pure_gender, count(*)
from sinchenko.genders
GROUP BY pure_gender;

/**
 	pure_gender	_c1
1	other	4221
2	female	512990
3	male	153100
**/

CREATE TABLE sinchenko.genders2 as
SELECT
country,
CASE WHEN pure_gender == "female" THEN 1
ELSE 0 END as col
from sinchenko.genders;

SELECT country, avg(col) as agg FROM genders2
GROUP BY country
ORDER BY agg desc;

/**
 	country	agg
1	Cote D'Ivoire	1
2	Virgin Islands	1
3	Afghanistan	1
4	Solomon Islands	1
5	Guam	1
6	Turkey	0.998825601879037
7	Nepal	0.9916317991631799
8	Samoa	0.987557479037057
9	India	0.9782840868636525
10	Pakistan	0.9617203500279278
11	Togo	0.9501568490763332
12	Liberia	0.9486552567237164
13	Benin	0.9476861167002012
14	Philippines	0.9472932874526232
15	Zimbabwe	0.9459593455627169
16	Vietnam	0.9358796936986807
17	Israel	0.9210526315789473
18	Kyrgyzstan	0.9087287842050572
19	Timor-Leste	0.8657493492004462
20	Malawi	0.8651515151515151
21	Senegal	0.8556511056511057
22	Sierra Leone	0.8531313504526141
23	Lesotho	0.8436018957345972
24	Cambodia	0.8289364145256208
25	Paraguay	0.8001683501683502
**/

/* 3. Объем финансирования */
SELECT
sector,
sum(loan_amount) as sum_,
avg(loan_amount) as avg_,
percentile(cast(loan_amount as BIGINT), 0.5) as median_
FROM sinchenko.kiva_loans_clean
GROUP BY sector
ORDER BY sum_ DESC;

/**
 	sector	sum_	avg_	median_
1	Agriculture	142806250	793.1917907131749	500
2	Food	121238875	888.3921374661097	450
3	Retail	97937175	787.5612158739094	425
4	Services	47918850	1063.5162127971237	550
5	Clothing	37225050	1138.6593050287531	600
6	Education	30936425	998.335646056538	725
7	Housing	23613400	701.6729563486168	500
8	Personal Use	14946450	410.8763779311103	200
9	Arts	12225425	1014.9792444997925	475
10	Transportation	11049875	712.7112358101135	450
11	Health	9809225	1065.6409560021727	725
12	Construction	6687525	1068.2947284345048	700
**/


SELECT
t2.world_region,
sum(t1.loan_amount) as sum_
FROM
kiva_loans_clean as t1
INNER JOIN kiva_mpi_region_locations as t2
ON t1.region = t2.region
GROUP BY t2.world_region
ORDER BY sum_ DESC;

/**
 	t2.world_region	sum_
1		138973961700
2	Europe and Central Asia	946618900
3	Arab States	315708025
4	Latin America and Caribbean	271793350
5	East Asia and the Pacific	163321225
6	South Asia	157590575
7	Sub-Saharan Africa	88539700
**/


