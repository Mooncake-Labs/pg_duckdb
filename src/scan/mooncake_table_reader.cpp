#include "duckdb/main/client_context.hpp"
#include "pgduckdb/scan/mooncake_table_reader.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" void mooncake_scan_table_begin(uint32_t table_id, uint64_t lsn, uint8_t **data, size_t *len);

extern "C" void mooncake_scan_table_end(uint32_t table_id, uint8_t *data, size_t len);

namespace pgduckdb {

MooncakeTableReader::MooncakeTableReader(uint32_t _table_id, uint64_t _lsn)
    : table_id(_table_id), lsn(_lsn), data(nullptr) {
}

MooncakeTableReader::~MooncakeTableReader() {
	if (data) {
		PostgresFunctionGuard(mooncake_scan_table_end, table_id, data, len);
	}
}

MooncakeTableMetadata &
MooncakeTableReader::GetTableMetadata(duckdb::ClientContext &context) {
	duckdb::lock_guard<duckdb::mutex> guard(lock);
	if (!data) {
		PostgresFunctionGuard(mooncake_scan_table_begin, table_id, lsn, &data, &len);
		metadata.Initialize(context, data, len);
	}
	return metadata;
}

} // namespace pgduckdb
