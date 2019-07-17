CREATE TABLE user_track.dwb_da_app_statistics (   id STRING,   app STRING,   app_version STRING,   platform STRING,   os_version STRING,   model_type STRING,   device_id STRING,   channel STRING,   user_ip STRING,   session_id STRING,   city STRING,   provider STRING,   role_type STRING,   login_account STRING,   network_type STRING,   screen STRING,   coordinate STRING,   time_version BIGINT,   op_time BIGINT,   page_id STRING,   page_name STRING,   ad_id STRING,   ad_desc STRING,   request_type STRING,   request_body STRING,   response STRING,   tel_no STRING,   object_id STRING,   event_obj STRING,   friend_id STRING,   start_time BIGINT,   end_time BIGINT,   pagination STRING,   target_page_id STRING,   code_channel STRING,   account_name STRING,   search_key STRING,   search_type STRING,   event_type STRING,   context_id STRING,   data_date STRING,   content STRING )ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;





