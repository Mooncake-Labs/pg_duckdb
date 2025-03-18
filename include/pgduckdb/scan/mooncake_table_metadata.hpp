#pragma once

#include "duckdb/common/unordered_set.hpp"

namespace pgduckdb {

class MooncakeTableMetadata {
public:
	MooncakeTableMetadata();

	~MooncakeTableMetadata();

	void Initialize(duckdb::ClientContext &context, uint8_t *data, size_t len);

	uint32_t
	GetNumDataFiles() {
		return data_files_len;
	}

	duckdb::string
	GetDataFile(uint32_t data_file_number) {
		return {data_files_data + data_files_offsets[data_file_number],
		        data_files_offsets[data_file_number + 1] - data_files_offsets[data_file_number]};
	}

	bool HasDeletes(uint32_t data_file_number);

	bool IsDeleted(uint32_t data_file_number, uint32_t data_file_row_number);

private:
	struct DeletionVector;

	using DataFileDelete = std::pair<duckdb::unique_ptr<DeletionVector>, bool /*has_position_deletes*/>;

	using PositionDelete = std::pair<uint32_t /*data_file_number*/, uint32_t /*data_file_row_number*/>;

	struct PositionDeleteHash {
		size_t
		operator()(const PositionDelete &val) const {
			return duckdb::CombineHash(duckdb::Hash(val.first), duckdb::Hash(val.second));
		}
	};

	uint32_t data_files_len;
	const uint32_t *data_files_offsets;
	const char *data_files_data;
	duckdb::vector<DataFileDelete> data_file_deletions;
	duckdb::unordered_set<PositionDelete, PositionDeleteHash> position_deletes;
};

}; // namespace pgduckdb
