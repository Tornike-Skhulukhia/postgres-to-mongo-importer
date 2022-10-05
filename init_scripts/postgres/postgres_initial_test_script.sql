-- using this simple script, we can test if tables created correctly on initial startap
CREATE database test_db;
\c test_db;
CREATE TABLE test_table(id BIGSERIAL, name text);
INSERT INTO test_table VALUES (1, 'Aaa'), (2, 'Bbb'), (3, 'Ccc');