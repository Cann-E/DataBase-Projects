DROP TABLE IF EXISTS T1;
DROP TABLE IF EXISTS T2;
DROP TABLE IF EXISTS T3;


CREATE TABLE T1 (k1 int, l1 int, A int,primary key(k1));
INSERT INTO T1 VALUES (1,3000,21);
INSERT INTO T1 VALUES (2,2000,22);
INSERT INTO T1 VALUES (3,2000,23);
INSERT INTO T1 VALUES (4,1000,22);

CREATE TABLE T2 (k2 int, l2 int, B int,primary key(k2));
INSERT INTO T2 VALUES (21,3,11);
INSERT INTO T2 VALUES (22,4,12);
INSERT INTO T2 VALUES (23,5,12);
INSERT INTO T2 VALUES (24,4,13);

CREATE TABLE T3 (k3 int, C int,primary key(k3));
INSERT INTO T3 VALUES (1,3);
INSERT INTO T3 VALUES (2,2);
INSERT INTO T3 VALUES (3,0);
