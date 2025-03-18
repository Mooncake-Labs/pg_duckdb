#include "pgduckdb/catalog/mooncake_table.hpp"
#include "duckdb/main/client_context.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" void mooncake_scan_table_begin(uint32_t table_id, uint64_t lsn, uint8_t **data, size_t *len);

extern "C" void mooncake_scan_table_end(uint32_t table_id, uint8_t *data, size_t len);

namespace pgduckdb {

MooncakeTable::~MooncakeTable() {
	if (data) {
		PostgresFunctionGuard(mooncake_scan_table_end, table_id, data, len);
	}
}

MooncakeTableMetadata &
MooncakeTable::GetTableMetadata(duckdb::ClientContext &context) {
	duckdb::lock_guard<duckdb::mutex> guard(lock);
	if (!data) {
		PostgresFunctionGuard(mooncake_scan_table_begin, table_id, lsn, &data, &len);
		metadata.Initialize(context, data, len);
	}
	return metadata;
}

} // namespace pgduckdb
