-- create 
CREATE database country_rivers;
\c country_rivers;

CREATE TABLE countries(code text, name text, continent text, capital_id int);
CREATE TABLE capital_cities(id int, name text, population int);
CREATE TABLE countries_rivers(id serial, country_code text, river_id int);
CREATE TABLE rivers(name text, length int, id int);

-- insert
INSERT INTO countries VALUES 
('GEO', 'Georgia', 'Europe', 905),
('CAN', 'Canada', 'North America', 1822),
('CHN', 'China', 'Asia', 1891),
('USA', 'United States', 'North America', 3813);

INSERT INTO capital_cities VALUES 
(1891, 'Beijing', 7472000),
(905, 'Tbilisi', 1235200),
(3813, 'Washington', 572059),
(1822, 'Ottawa', 335277);

INSERT INTO countries_rivers VALUES 
(1, 'GEO', 6),
(2, 'USA', 4),
(3, 'CAN', 4),
(4, 'CAN', 5),
(5, 'CHN', 9);

INSERT INTO rivers VALUES 
('Mississippi', 3766, 4),
('Mackenzie', 1740, 5),
('Mtkvari', 1515, 6),
('Yellow', 5464, 9);

-- add PK-s
ALTER TABLE ONLY countries 
ADD CONSTRAINT countries_pk PRIMARY KEY (code);

ALTER TABLE ONLY capital_cities 
ADD CONSTRAINT capital_cities_pk PRIMARY KEY (id);

ALTER TABLE ONLY countries_rivers 
ADD CONSTRAINT countries_rivers_pk PRIMARY KEY (id);

ALTER TABLE ONLY rivers 
ADD CONSTRAINT rivers_pk PRIMARY KEY (id);

-- add FK-s
ALTER TABLE ONLY countries 
ADD CONSTRAINT countries_capital_fk FOREIGN KEY (capital_id) REFERENCES capital_cities(id);

ALTER TABLE ONLY countries_rivers 
ADD CONSTRAINT countries_rivers_countries_fk FOREIGN KEY (country_code) REFERENCES countries(code);

ALTER TABLE ONLY countries_rivers 
ADD CONSTRAINT countries_rivers_rivers_fk FOREIGN KEY (river_id) REFERENCES rivers(id);


