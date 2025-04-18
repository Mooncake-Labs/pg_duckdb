#pragma once

#include "duckdb/common/common.hpp"

namespace pgduckdb {

struct MooncakeTableMetadata {
	uint32_t data_files_len = 0;
	const uint32_t *data_files_offsets = nullptr;
	const char *data_files_data = nullptr;
	uint32_t position_deletes_len = 0;
	const uint32_t *position_deletes_data = nullptr;

	duckdb::string
	GetDataFile(duckdb::idx_t i) {
		return {data_files_data + data_files_offsets[i], data_files_offsets[i + 1] - data_files_offsets[i]};
	}

	std::pair<uint32_t, uint32_t>
	GetPositionDelete(duckdb::idx_t i) {
		return {position_deletes_data[2 * i], position_deletes_data[2 * i + 1]};
	}
};

class MooncakeScanFfi {
public:
	MooncakeScanFfi(duckdb::string schema, duckdb::string table);

	~MooncakeScanFfi();

	MooncakeTableMetadata GetTableMetadata() const;

private:
	duckdb::string schema;
	duckdb::string table;
	uint8_t *data;
	size_t len;
};

} // namespace pgduckdb
