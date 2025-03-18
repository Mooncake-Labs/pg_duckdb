#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace pgduckdb {

class MooncakeTableMetadata {
public:
	MooncakeTableMetadata(uint8_t *data, size_t len);

	uint32_t
	GetNumDataFiles() {
		return data_files_len;
	}

	duckdb::string
	GetDataFile(uint32_t data_file_number) {
		return {data_files_data + data_files_offsets[data_file_number],
		        data_files_offsets[data_file_number + 1] - data_files_offsets[data_file_number]};
	}

	duckdb::unique_ptr<duckdb::DeleteFilter> GetDeleteFilter(duckdb::ClientContext &context, uint32_t data_file_number);

private:
	// [data_file_number, puffin_file_number, offset, size]
	using DeletionVector = uint32_t[4];
	// [data_file_number, data_file_row_number]
	using PositionDelete = uint32_t[2];

	uint32_t data_files_len;
	const uint32_t *data_files_offsets;
	const char *data_files_data;
	uint32_t puffin_files_len;
	const uint32_t *puffin_files_offsets;
	const char *puffin_files_data;
	uint32_t deletion_vectors_len;
	DeletionVector *deletion_vectors;
	uint32_t position_deletes_len;
	PositionDelete *position_deletes;
};

}; // namespace pgduckdb
