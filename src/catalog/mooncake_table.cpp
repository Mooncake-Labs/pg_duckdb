#include "pgduckdb/catalog/mooncake_table.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" void mooncake_scan_table_begin(const char *schema, const char *table, uint64_t lsn, uint8_t **data,
                                          size_t *len);

extern "C" void mooncake_scan_table_end(const char *schema, const char *table, uint8_t *data, size_t len);

namespace pgduckdb {

MooncakeTable::~MooncakeTable() {
	if (metadata) {
		PostgresFunctionGuard(mooncake_scan_table_end, schema.name.c_str(), name.c_str(), data, len);
	}
}

MooncakeTableMetadata &
MooncakeTable::GetTableMetadata() {
	duckdb::lock_guard<duckdb::mutex> guard(lock);
	if (!metadata) {
		PostgresFunctionGuard(mooncake_scan_table_begin, schema.name.c_str(), name.c_str(), lsn, &data, &len);
		metadata = duckdb::make_uniq<MooncakeTableMetadata>(data, len);
	}
	return *metadata;
}

} // namespace pgduckdb
