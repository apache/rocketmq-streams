/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE IF NOT EXISTS  `window_max_value` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL,
  `gmt_modified` datetime NOT NULL,
  `max_value` bigint(20) unsigned NOT NULL,
  `max_event_time` bigint(20) unsigned NOT NULL,
  `msg_key` varchar(256) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk__ket` (`msg_key`(250)),
  KEY `idx_modifytime` (`gmt_modified`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `window_value` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL,
  `gmt_modified` datetime NOT NULL,
  `start_time` varchar(20) NOT NULL,
  `end_time` varchar(20) NOT NULL,
  `max_offset` longtext,
  `group_by` text,
  `agg_column_result` longtext,
  `computed_column_result` longtext,
  `version` varchar(64) DEFAULT NULL,
  `name_space` varchar(256) DEFAULT NULL,
  `configure_name` varchar(256) DEFAULT NULL,
  `msg_key` varchar(64) NOT NULL,
  `window_instance_id` varchar(64) NOT NULL,
  `partition` varchar(512) DEFAULT NULL,
  `partition_num` bigint(20) DEFAULT NULL,
  `fire_time` varchar(20) DEFAULT NULL,
  `update_version` bigint(20) unsigned DEFAULT NULL,
  `update_flag` bigint(20) DEFAULT NULL,
  `window_instance_partition_id` varchar(64) DEFAULT NULL,
  `type` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_window_state` (`msg_key`),
  KEY `idx_window_instance_shuffle` (`window_instance_partition_id`,`partition_num`),
  KEY `idx_window_instance_firetime` (`window_instance_partition_id`,`fire_time`),
  KEY `idx_window` (`name_space`(128),`configure_name`(128),`partition`(128))
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `window_task` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `gmt_modified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `task_id` varchar(64) NOT NULL,
  `untreated_flag` int(11) NOT NULL DEFAULT '0',
  `group_by_value` varchar(1024) NOT NULL,
  `task_owner` varchar(256) NOT NULL,
  `task_send_time` datetime DEFAULT NULL,
  `send_task_msg` text NOT NULL,
  `msg_send_time` bigint(20) DEFAULT NULL,
  `name` varchar(128) NOT NULL,
  `start_time` varchar(20) NOT NULL,
  `end_time` varchar(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_taskid` (`task_id`),
  KEY `idx_flag_modifytime` (`name`,`untreated_flag`,`gmt_modified`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `window_instance` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL,
  `gmt_modified` datetime NOT NULL,
  `start_time` varchar(20) NOT NULL,
  `end_time` varchar(20) NOT NULL,
  `fire_time` varchar(20) NOT NULL,
  `window_name` varchar(128) NOT NULL,
  `window_name_space` varchar(128) NOT NULL,
  `status` tinyint(4) NOT NULL DEFAULT '0',
  `version` int(11) DEFAULT '0',
  `window_instance_key` varchar(128) DEFAULT NULL,
  `window_instance_name` varchar(128) DEFAULT NULL,
  `window_Instance_split_name` varchar(128) DEFAULT NULL,
  `split_id` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_window_instance_uniq_index` (`window_instance_key`),
  KEY `idx_gmt_modified` (`fire_time`,`window_name`,`window_name_space`,`status`),
  KEY `idx_windowinstance_name` (`window_instance_name`),
  KEY `idx_windowinstance_split_name` (`window_Instance_split_name`),
  KEY `idx_windowinstance_split_name_firetime` (`window_Instance_split_name`,`fire_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `lease_info` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL,
  `gmt_modified` datetime NOT NULL,
  `lease_name` varchar(255) NOT NULL,
  `lease_user_ip` varchar(255) NOT NULL,
  `lease_end_time` varchar(255) NOT NULL,
  `status` int(11) NOT NULL DEFAULT '1',
  `version` bigint(20) NOT NULL,
  `candidate_lease_ip` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`lease_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `dipper_sql_configure` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL,
  `gmt_modified` datetime NOT NULL,
  `namespace` varchar(32) NOT NULL,
  `type` varchar(32) NOT NULL,
  `name` varchar(128) NOT NULL,
  `json_value` longtext NOT NULL,
  `request_id` varchar(128) NOT NULL,
  `account_id` varchar(32) NOT NULL,
  `account_name` varchar(32) NOT NULL,
  `account_nickname` varchar(32) NOT NULL,
  `client_ip` varchar(64) NOT NULL,
  `status` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `is_publish` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_namespace_type_name` (`namespace`,`type`,`name`),
  KEY `idx_namespace` (`namespace`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `dipper_configure` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime NOT NULL,
  `gmt_modified` datetime NOT NULL,
  `namespace` varchar(32) NOT NULL,
  `type` varchar(32) NOT NULL,
  `name` varchar(128) NOT NULL,
  `json_value` text NOT NULL,
  `request_id` varchar(128) NOT NULL,
  `account_id` varchar(32) NOT NULL,
  `account_name` varchar(32) NOT NULL,
  `account_nickname` varchar(32) NOT NULL,
  `client_ip` varchar(64) NOT NULL,
  `status` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `isPublish` int(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_namespace_type_name` (`namespace`,`type`,`name`),
  KEY `idx_namespace` (`namespace`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `join_right_state` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime DEFAULT NULL,
  `gmt_modified` datetime DEFAULT NULL,
  `window_id` bigint(20) DEFAULT NULL,
  `window_name` varchar(200) DEFAULT NULL,
  `window_name_space` varchar(45) DEFAULT NULL,
  `message_id` varchar(200) DEFAULT NULL,
  `message_key` varchar(32) DEFAULT NULL,
  `message_time` datetime DEFAULT NULL,
  `message_body` longtext,
  `msg_key` varchar(400) DEFAULT NULL,
  `window_instance_id` varchar(200) DEFAULT NULL,
  `partition` varchar(200) DEFAULT NULL,
  `partition_num` bigint(20) DEFAULT NULL,
  `window_instance_partition_id` varchar(200) DEFAULT NULL,
  `version` varchar(64) DEFAULT NULL,
  `update_flag` bigint(20) DEFAULT NULL,
  `name_space` varchar(256) DEFAULT NULL,
  `configure_name` varchar(256) DEFAULT NULL,
  `type` varchar(64) DEFAULT NULL,
  `name` varchar(64) DEFAULT NULL,
  `update_version` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_message_id_unique` (`message_id`),
  KEY `idx_message_key_index` (`message_key`),
  KEY `idx_gmt_create_index` (`gmt_create`),
  KEY `idx_window_name_index` (`window_name`(70)),
  KEY `idx_message_key_gmt_create_index` (`message_key`,`gmt_create`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `join_left_state` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `gmt_create` datetime DEFAULT NULL,
  `gmt_modified` datetime DEFAULT NULL,
  `window_id` bigint(20) DEFAULT NULL,
  `window_name` varchar(200) DEFAULT NULL,
  `window_name_space` varchar(45) DEFAULT NULL,
  `message_id` varchar(200) DEFAULT NULL,
  `message_key` varchar(32) DEFAULT NULL,
  `message_time` datetime DEFAULT NULL,
  `message_body` longtext,
  `msg_key` varchar(400) DEFAULT NULL,
  `window_instance_id` varchar(200) DEFAULT NULL,
  `partition` varchar(200) DEFAULT NULL,
  `partition_num` bigint(20) DEFAULT NULL,
  `window_instance_partition_id` varchar(200) DEFAULT NULL,
  `version` varchar(64) DEFAULT NULL,
  `update_flag` bigint(20) DEFAULT NULL,
  `name_space` varchar(256) DEFAULT NULL,
  `configure_name` varchar(256) DEFAULT NULL,
  `type` varchar(64) DEFAULT NULL,
  `name` varchar(64) DEFAULT NULL,
  `update_version` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_message_id_unique` (`message_id`),
  KEY `idx_message_key_index` (`message_key`),
  KEY `idx_gmt_create_index` (`gmt_create`),
  KEY `idx_window_name_index` (`window_name`(70)),
  KEY `idx_message_key_gmt_create_index` (`message_key`,`gmt_create`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

