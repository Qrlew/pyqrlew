-- Install census table
CREATE TABLE "census" ("age" integer, "workclass" varchar(20), "fnlwgt" varchar(20), "education" varchar(20), "education_num" integer, "marital_status" varchar(30), "occupation" varchar(20), "relationship" varchar(20), "race" varchar(20), "sex" varchar(20), "capital_gain" integer, "capital_loss" integer, "hours_per_week" integer, "native_country" varchar(30), "income" varchar(20));
\copy "census" ("age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country", "income") FROM 'tests/ressources/install_db/extract_census.csv' CSV DELIMITER ',' HEADER;
-- Install beacon table
CREATE TABLE "beacon" ("検知日時" timestamp, "UserId" varchar(35), "所属部署" varchar, "フロア名" varchar, "Beacon名" varchar, "RSSI" integer, "マップのX座標" integer, "マップのY座標" integer);
\copy "beacon" ("検知日時","UserId","所属部署","フロア名","Beacon名","RSSI","マップのX座標","マップのY座標") FROM 'tests/ressources/install_db/extract_beacon.csv' CSV DELIMITER ',';
