#!/bin/sh

mysql -utest -ptest <<!
delete from poc.test2;
delete from poc.test3;
!

sqoop export --connect jdbc:mysql://master/poc --table test2 --username test --password test --export-dir /user/root/xiaoy/test2 --fields-terminated-by '|'
sqoop export --connect jdbc:mysql://master/poc --table test3 --username test --password test --export-dir /user/root/xiaoy/test3 --fields-terminated-by '|'
