#include "pgduckdb/catalog/mooncake_table.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" void mooncake_scan_table_begin(uint32_t table_id, uint64_t lsn, uint8_t **data, size_t *len);

extern "C" void mooncake_scan_table_end(uint32_t table_id, uint8_t *data, size_t len);

namespace pgduckdb {

MooncakeTable::~MooncakeTable() {
	if (metadata) {
		PostgresFunctionGuard(mooncake_scan_table_end, table_id, data, len);
	}
}

MooncakeTableMetadata &
MooncakeTable::GetTableMetadata() {
	duckdb::lock_guard<duckdb::mutex> guard(lock);
	if (!metadata) {
		PostgresFunctionGuard(mooncake_scan_table_begin, table_id, lsn, &data, &len);
		metadata = duckdb::make_uniq<MooncakeTableMetadata>(data, len);
	}
	return *metadata;
}

} // namespace pgduckdb
