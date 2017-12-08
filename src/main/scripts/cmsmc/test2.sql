drop table if exists test.test2;
create external table test.test2 
(warn_time string, warn_obj string, warn_amt string, threshold_amt string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE 
location '/user/root/xiaoy/test2'
;
