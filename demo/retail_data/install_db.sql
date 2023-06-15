DROP DATABASE IF EXISTS retail;
CREATE DATABASE retail;
\c retail;
SET DateStyle TO DMY;
-- Install stores table
CREATE TABLE stores (Store varchar(10) PRIMARY KEY, Type char(1) NOT NULL, Size integer NOT NULL);
\copy stores (Store, Type, Size) FROM 'retail_data/data/stores.csv' CSV DELIMITER ',' HEADER;
-- Install features table
CREATE TABLE features (id varchar(10) UNIQUE, Store varchar(10) REFERENCES stores (Store) NOT NULL, Date timestamp, Temperature float NOT NULL, Fuel_Price float NOT NULL, CPI float, Unemployment float, IsHoliday boolean NOT NULL);
\copy features (id, Store, Date, Temperature, Fuel_Price, CPI, Unemployment, IsHoliday) FROM 'retail_data/data/features.csv' CSV DELIMITER ',' HEADER;
-- Install sales table
CREATE TABLE sales (id varchar(10) UNIQUE, Store varchar(10) REFERENCES stores (Store) NOT NULL, Date timestamp NOT NULL, Weekly_Sales float NOT NULL, IsHoliday boolean NOT NULL);
\copy sales (id, Store, Date, Weekly_Sales, IsHoliday) FROM 'retail_data/data/sales.csv' CSV DELIMITER ',' HEADER;
