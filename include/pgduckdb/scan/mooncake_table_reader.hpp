#pragma once

#include "pgduckdb/scan/mooncake_table_metadata.hpp"

namespace pgduckdb {

class MooncakeTableReader {
public:
	MooncakeTableReader(uint32_t table_id, uint64_t lsn);

	~MooncakeTableReader();

	MooncakeTableMetadata &GetTableMetadata(duckdb::ClientContext &context);

private:
	uint32_t table_id;
	uint64_t lsn;

	duckdb::mutex lock;
	uint8_t *data;
	size_t len;
	MooncakeTableMetadata metadata;
};

}; // namespace pgduckdb
