# Requirements Document

## Introduction

A comprehensive data collection system for Binance cryptocurrency market data that retrieves OHLCV data across multiple timeframes and trading pairs, along with Top Trader position and account data. The system handles rate limiting, data integrity, and automated backups while supporting all Binance trading categories (Spot, USDT-M, COIN-M).

## Glossary

- **Binance_Data_Collector**: The main system responsible for collecting and storing cryptocurrency market data from Binance
- **OHLCV_Data**: Open, High, Low, Close, Volume market data for specific timeframes
- **Top_Trader_Data**: Position and account data from top traders on Binance futures markets
- **Symbol_Manager**: Component responsible for maintaining current list of trading symbols
- **Rate_Limiter**: Component that manages API request frequency to avoid exceeding Binance limits
- **Backup_System**: Component that creates and manages data backups
- **WebSocket_Client**: Real-time data streaming client for 1-minute OHLCV data
- **REST_Client**: HTTP client for retrieving historical and periodic data
- **SQLite_Database**: Local database for storing collected market data

## Requirements

### Requirement 1

**User Story:** As a cryptocurrency data analyst, I want to collect 1-minute OHLCV data via WebSocket for all Binance symbols, so that I can analyze real-time market movements.

#### Acceptance Criteria

1. WHEN the Binance_Data_Collector starts, THE WebSocket_Client SHALL establish connections for 1-minute OHLCV streams
2. WHILE collecting 1-minute data, THE WebSocket_Client SHALL maintain maximum 300 symbols per stream connection
3. WHEN receiving OHLCV data, THE Binance_Data_Collector SHALL store the data in SQLite_Database with proper timestamp indexing
4. IF a WebSocket connection fails, THEN THE WebSocket_Client SHALL automatically reconnect within 5 seconds
5. THE Binance_Data_Collector SHALL collect 1-minute OHLCV data for Spot, USDT-M, and COIN-M trading pairs

### Requirement 2

**User Story:** As a cryptocurrency data analyst, I want to collect 30-minute and daily OHLCV data via REST API, so that I can perform longer-term market analysis.

#### Acceptance Criteria

1. THE REST_Client SHALL retrieve 30-minute and daily OHLCV data for all Binance symbols
2. WHEN making REST requests, THE Rate_Limiter SHALL ensure requests stay within Binance API limits
3. THE Binance_Data_Collector SHALL update 30-minute and daily data at appropriate intervals
4. WHEN storing REST data, THE SQLite_Database SHALL prevent duplicate entries using timestamp and symbol keys
5. IF a REST request fails due to rate limiting, THEN THE Rate_Limiter SHALL implement exponential backoff retry logic

### Requirement 3

**User Story:** As a cryptocurrency trader, I want to collect Top Trader Positions and Account data for USDT-M symbols, so that I can analyze institutional trading patterns.

#### Acceptance Criteria

1. THE REST_Client SHALL retrieve Top Trader Positions data for all USDT-M symbols every 5 minutes
2. THE REST_Client SHALL retrieve Top Trader Accounts data for all USDT-M symbols every 5 minutes
3. WHEN collecting Top Trader data, THE Binance_Data_Collector SHALL store position ratios and account ratios separately
4. THE SQLite_Database SHALL maintain historical Top Trader data with 5-minute granularity
5. WHILE collecting Top Trader data, THE Rate_Limiter SHALL coordinate with OHLCV collection to avoid exceeding limits

### Requirement 4

**User Story:** As a system administrator, I want the symbol list to be updated daily, so that new and delisted trading pairs are automatically handled.

#### Acceptance Criteria

1. THE Symbol_Manager SHALL update the symbol list once per day using Exchange Information endpoint
2. WHEN updating symbols, THE Symbol_Manager SHALL identify new symbols and add them to data collection
3. WHEN updating symbols, THE Symbol_Manager SHALL identify delisted symbols and mark them as inactive
4. THE SQLite_Database SHALL store symbol metadata including status, base asset, and quote asset
5. AFTER symbol updates, THE Binance_Data_Collector SHALL adjust WebSocket and REST collection accordingly

### Requirement 5

**User Story:** As a data analyst, I want all collected data to be stored in ~/Volume/buffakohd with automatic backups, so that I can ensure data persistence and recovery.

#### Acceptance Criteria

1. THE SQLite_Database SHALL store all collected data in ~/Volume/buffakohd directory
2. THE Backup_System SHALL create daily backups of the SQLite database
3. WHEN performing database writes, THE SQLite_Database SHALL use transaction locks to prevent data corruption
4. THE Backup_System SHALL maintain backup files with timestamp naming convention
5. IF database corruption is detected, THEN THE Backup_System SHALL provide recovery options from recent backups

### Requirement 6

**User Story:** As a system operator, I want robust rate limiting and error handling, so that the system operates reliably without API violations.

#### Acceptance Criteria

1. THE Rate_Limiter SHALL track API usage across all endpoints and enforce Binance rate limits
2. WHEN rate limits are approached, THE Rate_Limiter SHALL queue requests and delay execution
3. IF rate limit violations occur, THEN THE Rate_Limiter SHALL implement exponential backoff with maximum 60-second delays
4. THE Binance_Data_Collector SHALL log all rate limiting events and retry attempts
5. WHILE handling errors, THE Binance_Data_Collector SHALL continue operating for unaffected data streams