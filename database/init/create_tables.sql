-- Criacao do table
CREATE TABLE IF NOT EXISTS raw_tokens (
	id SERIAL,
	address VARCHAR(250),
	symbol VARCHAR(250),
	name VARCHAR(250),
	decimals VARCHAR(250),
	total_supply VARCHAR(250),
	block_timestamp TIMESTAMP,
	block_number INT,
	block_hash VARCHAR(250),
	PRIMARY KEY (id)
);