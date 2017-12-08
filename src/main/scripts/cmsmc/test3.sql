drop table if exists test.test3;
create external table test.test3 
(warn_time string, warn_obj string, warn_pct string, thresh_pct string, warn_rate string, thresh_rate string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE 
location '/user/root/xiaoy/test3'
;
