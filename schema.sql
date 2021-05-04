
--
-- Table structure for table `token_snapshots`
--
DROP TABLE IF EXISTS `token_snapshots`;
CREATE TABLE `token_snapshots` (
  `row_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `block_number` bigint(20) NOT NULL,
  `balance_in_cogs` bigint(20) NOT NULL,
  `snapshot_date` timestamp NOT NULL,
  `wallet_address` varchar(50) NOT NULL,
  `is_contract` bit(1) DEFAULT 0,
  `row_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `row_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  
  PRIMARY KEY (`row_id`),
  UNIQUE KEY `uq_sn` (`wallet_address`),
  KEY `ix_token_snapshots_wallet_address` (`wallet_address`)
) ENGINE=InnoDB AUTO_INCREMENT=50277 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `token_transfer_validation`
--
DROP TABLE IF EXISTS `token_transfer_validation`;
CREATE TABLE `token_transfer_validation` (
  `row_id` int(11) NOT NULL AUTO_INCREMENT,
  `wallet_address` varchar(45) DEFAULT NULL,
  `is_contract` bit(1) DEFAULT 0, 
  `snapshot_balance_in_cogs` bigint(20) DEFAULT NULL,
  `transfer_balance_in_cogs` bigint(20) DEFAULT NULL,
  `row_created` timestamp NULL DEFAULT NULL,
  `row_updated` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`row_id`),
  UNIQUE KEY `wallet_address_UNIQUE` (`wallet_address`)
) ENGINE=InnoDB AUTO_INCREMENT=51203 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Table structure for table `transfer_info`
--
DROP TABLE IF EXISTS `transfer_info`;
CREATE TABLE `transfer_info` (
  `row_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `row_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `row_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `wallet_address` varchar(50) NOT NULL,
  `transfer_time` timestamp NOT NULL,
  `transfer_fees` bigint(20) NOT NULL,
  `transfer_transaction` varchar(255) NOT NULL,
  `transfer_amount_in_cogs` bigint(20) NOT NULL,
  `transfer_status` varchar(50) NOT NULL,
  `is_contract` bit(1) NOT NULL,
  PRIMARY KEY (`row_id`),
  KEY `ix_transfer_info_wallet_address` (`wallet_address`)
) ENGINE=InnoDB AUTO_INCREMENT=25659 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
