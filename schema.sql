--AUTHOR: TERRY ADEAGBO
--DESCRIPTION: PARSER SCHEMA CREATION AND DATA LOADING BASED ON DYNAMIC PARTITIONS
--COMMENTS: Version 0.1

CREATE DATABASE IF NOT EXISTS staging;
USE staging;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS blockfieldmessage(
	uuid			STRING,
	country 		STRING,
	correspondent 	STRING,
	direction 		STRING,
	department 		STRING,
	irn 			STRING,
	trn 			STRING,
	isn 			STRING,
	osn 			STRING,
	ccy 			STRING,
	amount 			DOUBLE,
	sender_bic 		STRING,
	block 			STRING,
	field 			STRING,
	values 			STRING
)
COMMENT 'on-demand message in block,field, values format'
PARTITIONED BY(record_date STRING, region STRING,message_type STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS messageaudit(
	uuid			STRING,
	country 		STRING,
	correspondent 	STRING,
	timestamp 		STRING,
	code 			STRING,
	message_text 	STRING
)
COMMENT 'on-demand message audit'
PARTITIONED BY(record_date STRING, region STRING,message_type STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS mtmessage(
	uuid 				STRING,
	country 			STRING,
	correspondent 		STRING,
	encoded_text 		STRING
)
COMMENT 'on-demand message in BASE64 format'
PARTITIONED BY(record_date STRING, region STRING,message_type STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS ondemandstats(
	timestamp 								STRING,
	filename 								STRING,
	region 									STRING,
	input_file_size 						INT,
	output_file_size 						INT,
	usable_disk_space 						INT,
	total_message_count 					INT,
	failed_message_count 					INT,
	success_dtcs_count						INT,
	success_scope_count						INT,
	success_dtcs_scope_message_stats		MAP<STRING,STRING>,
	success_dtcs_scope_country_stats		MAP<STRING,STRING>,
	success_dtcs_scope_bic_stats			MAP<STRING,STRING>
)
COMMENT 'on-demand MT Parser stats'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY '\;' 
MAP KEYS TERMINATED BY ':'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TEMPORARY TABLE IF NOT EXISTS temp_blockfieldmessage(
	uuid			STRING,
	record_date 	STRING, 
	region 			STRING,
	message_type 	STRING,
	country 		STRING,
	correspondent 	STRING,
	direction 		STRING,
	department 		STRING,
	irn 			STRING,
	trn 			STRING,
	isn 			STRING,
	osn 			STRING,
	ccy 			STRING,
	amount 			DOUBLE,
	sender_bic 		STRING,
	block 			STRING,
	field 			STRING,
	values 			STRING
)
COMMENT 'temporary table for loading partition data into blockfieldmessage table '
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/staging.db/temp_blockfieldmessage';

CREATE TEMPORARY TABLE IF NOT EXISTS temp_messageaudit(
	uuid			STRING,
	record_date 	STRING, 
	region 			STRING,
	message_type 	STRING,
	country 		STRING,
	correspondent 	STRING,
	timestamp 		STRING,
	code 			STRING,
	message_text 	STRING
)
COMMENT 'temporary table for loading partition data into messageaudit table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/staging.db/temp_messageaudit';


CREATE TEMPORARY TABLE IF NOT EXISTS temp_mtmessage(
	uuid 				STRING,
	record_date 		STRING, 
	region 				STRING,
	message_type 		STRING,
	country 			STRING,
	correspondent 		STRING,
	encoded_text 		STRING
)
COMMENT 'temporary table for loading partition data into mtmessage table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/staging.db/temp_mtmessage';

LOAD DATA LOCAL INPATH 'messageSWIFT.txt_stats' INTO TABLE ondemandstats;

LOAD DATA LOCAL INPATH 'messageSWIFT.txt_blockfield' INTO TABLE temp_blockfieldmessage;
LOAD DATA LOCAL INPATH 'messageSWIFT.txt_audit' INTO TABLE temp_messageaudit;
LOAD DATA LOCAL INPATH 'messageSWIFT.txt_message' INTO TABLE temp_mtmessage;

INSERT OVERWRITE TABLE blockfieldmessage PARTITION(record_date , region ,message_type )
SELECT 
	uuid,
	country,
	correspondent,
	direction,
	department,
	irn,
	trn,
	isn,
	osn,
	ccy,
	amount,
	sender_bic,
	block,
	field,
	values,
	record_date,
	region,
	message_type
FROM temp_blockfieldmessage;

INSERT OVERWRITE TABLE messageaudit PARTITION(record_date , region ,message_type )
SELECT 
	uuid,
	country,
	correspondent,
	code,
	timestamp,
	message_text,
	record_date, 
	region,
	message_type
FROM temp_messageaudit;

INSERT OVERWRITE TABLE mtmessage PARTITION(record_date , region ,message_type )
SELECT 
	uuid,
	country,
	correspondent,
	encoded_text,
	record_date,
	region,
	message_type 
FROM temp_mtmessage;
