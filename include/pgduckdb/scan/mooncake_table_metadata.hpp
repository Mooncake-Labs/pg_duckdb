#pragma once

#include "duckdb/common/common.hpp"

namespace pgduckdb {

struct MooncakeTableMetadata {
	struct DataFiles {
		DataFiles() : len(0), offsets(nullptr), data(nullptr) {
		}

		uint32_t len;
		uint32_t *offsets;
		char *data;

		duckdb::string
		Get(duckdb::idx_t i) {
			return {data + offsets[i], offsets[i + 1] - offsets[i]};
		}
	};

	struct PositionDeletes {
		PositionDeletes() : len(0), data(nullptr) {
		}

		uint32_t len;
		uint32_t *data;

		std::pair<uint32_t, uint32_t>
		Get(duckdb::idx_t i) {
			return {data[2 * i], data[2 * i + 1]};
		}
	};

	DataFiles data_files;
	PositionDeletes position_deletes;
};

MooncakeTableMetadata GetMooncakeTableMetadata();

} // namespace pgduckdb
