-- Oracle Test Data Loader - Generated 2025-11-26T23:53:31.273Z
-- Usage: sqlplus user/pass@db @this_file.sql

SET ECHO OFF
SET FEEDBACK OFF

-- Drop and create string_tests
BEGIN EXECUTE IMMEDIATE 'DROP TABLE string_tests CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE string_tests (id VARCHAR2(50) PRIMARY KEY, data JSON);

INSERT INTO string_tests (id, data) VALUES ('1', '{"_id":1,"str":"hello","str2":"world","empty":"","mixed":"HeLLo WoRLD"}');
INSERT INTO string_tests (id, data) VALUES ('2', '{"_id":2,"str":"UPPERCASE","str2":"lowercase","empty":"","mixed":"123abc"}');
INSERT INTO string_tests (id, data) VALUES ('3', '{"_id":3,"str":"  trimme  ","str2":"  spaces  ","empty":null,"mixed":"MiXeD"}');
INSERT INTO string_tests (id, data) VALUES ('4', '{"_id":4,"str":"abcdefghij","str2":"klmnop","num":12345}');
INSERT INTO string_tests (id, data) VALUES ('5', '{"_id":5,"str":"","str2":"","empty":""}');

-- Drop and create array_tests
BEGIN EXECUTE IMMEDIATE 'DROP TABLE array_tests CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE array_tests (id VARCHAR2(50) PRIMARY KEY, data JSON);

INSERT INTO array_tests (id, data) VALUES ('1', '{"_id":1,"arr":[1,2,3,4,5],"tags":["a","b","c"]}');
INSERT INTO array_tests (id, data) VALUES ('2', '{"_id":2,"arr":["x","y","z"],"tags":["single"]}');
INSERT INTO array_tests (id, data) VALUES ('3', '{"_id":3,"arr":[],"tags":[]}');
INSERT INTO array_tests (id, data) VALUES ('4', '{"_id":4,"arr":[10],"tags":["one","two","three","four"]}');
INSERT INTO array_tests (id, data) VALUES ('5', '{"_id":5,"arr":[[1,2],[3,4]],"tags":["nested"]}');
INSERT INTO array_tests (id, data) VALUES ('6', '{"_id":6,"arr":[null,1,null,2],"tags":["with","nulls"]}');

-- Drop and create cond_tests
BEGIN EXECUTE IMMEDIATE 'DROP TABLE cond_tests CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE cond_tests (id VARCHAR2(50) PRIMARY KEY, data JSON);

INSERT INTO cond_tests (id, data) VALUES ('1', '{"_id":1,"score":95,"status":"active","value":100}');
INSERT INTO cond_tests (id, data) VALUES ('2', '{"_id":2,"score":75,"status":"active","value":50}');
INSERT INTO cond_tests (id, data) VALUES ('3', '{"_id":3,"score":55,"status":"inactive","value":null}');
INSERT INTO cond_tests (id, data) VALUES ('4', '{"_id":4,"score":45,"status":"active","value":0}');
INSERT INTO cond_tests (id, data) VALUES ('5', '{"_id":5,"score":85,"status":"pending","value":-10}');

-- Drop and create date_tests
BEGIN EXECUTE IMMEDIATE 'DROP TABLE date_tests CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE date_tests (id VARCHAR2(50) PRIMARY KEY, data JSON);

INSERT INTO date_tests (id, data) VALUES ('1', '{"_id":1,"date":"2024-01-15T10:30:45.123Z","name":"January morning"}');
INSERT INTO date_tests (id, data) VALUES ('2', '{"_id":2,"date":"2024-06-21T14:00:00.000Z","name":"June afternoon"}');
INSERT INTO date_tests (id, data) VALUES ('3', '{"_id":3,"date":"2024-12-25T00:00:00.000Z","name":"Christmas midnight"}');
INSERT INTO date_tests (id, data) VALUES ('4', '{"_id":4,"date":"2024-02-29T23:59:59.999Z","name":"Leap day end"}');
INSERT INTO date_tests (id, data) VALUES ('5', '{"_id":5,"date":"2024-07-04T12:00:00.000Z","name":"July 4th noon"}');

-- Drop and create arith_tests
BEGIN EXECUTE IMMEDIATE 'DROP TABLE arith_tests CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE arith_tests (id VARCHAR2(50) PRIMARY KEY, data JSON);

INSERT INTO arith_tests (id, data) VALUES ('1', '{"_id":1,"a":10,"b":3}');
INSERT INTO arith_tests (id, data) VALUES ('2', '{"_id":2,"a":0,"b":5}');
INSERT INTO arith_tests (id, data) VALUES ('3', '{"_id":3,"a":-10,"b":3}');
INSERT INTO arith_tests (id, data) VALUES ('4', '{"_id":4,"a":100,"b":0}');
INSERT INTO arith_tests (id, data) VALUES ('5', '{"_id":5,"a":7.5,"b":2.5}');
INSERT INTO arith_tests (id, data) VALUES ('6', '{"_id":6,"a":null,"b":5}');

-- Drop and create acc_tests
BEGIN EXECUTE IMMEDIATE 'DROP TABLE acc_tests CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE acc_tests (id VARCHAR2(50) PRIMARY KEY, data JSON);

INSERT INTO acc_tests (id, data) VALUES ('1', '{"_id":1,"group":"A","value":10,"status":"active"}');
INSERT INTO acc_tests (id, data) VALUES ('2', '{"_id":2,"group":"A","value":20,"status":"active"}');
INSERT INTO acc_tests (id, data) VALUES ('3', '{"_id":3,"group":"A","value":30,"status":"inactive"}');
INSERT INTO acc_tests (id, data) VALUES ('4', '{"_id":4,"group":"B","value":5,"status":"active"}');
INSERT INTO acc_tests (id, data) VALUES ('5', '{"_id":5,"group":"B","value":null,"status":"active"}');
INSERT INTO acc_tests (id, data) VALUES ('6', '{"_id":6,"group":"C","value":100,"status":"pending"}');

COMMIT;

