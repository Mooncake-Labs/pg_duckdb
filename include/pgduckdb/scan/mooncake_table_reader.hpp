#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"

namespace pgduckdb {

struct MooncakeTableMetadata {
	uint32_t data_files_len = 0;
	const uint32_t *data_files_offsets = nullptr;
	const char *data_files_data = nullptr;
	uint32_t deletion_vectors_len = 0;
	const uint32_t *deletion_vectors_offsets = nullptr;
	const uint32_t *deletion_vectors_blobs = nullptr;
	const char *deletion_vectors_data = nullptr;
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

class MooncakeTableReader {
public:
	MooncakeTableReader(uint32_t table_id, uint64_t lsn);

	~MooncakeTableReader();

	MooncakeTableMetadata LazyGetTableMetadata();

private:
	uint32_t table_id;
	uint64_t lsn;
	uint8_t *data;
	size_t len;
	duckdb::mutex lock;
	MooncakeTableMetadata metadata;
};

}; // namespace pgduckdb
