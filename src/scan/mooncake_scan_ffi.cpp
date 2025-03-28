#include "pgduckdb/scan/mooncake_scan_ffi.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" void mooncake_scan_begin(const char *schema, const char *table, uint8_t **data, size_t *len);

extern "C" void mooncake_scan_end(uint8_t *data, size_t len);

namespace pgduckdb {

MooncakeScanFfi::MooncakeScanFfi(const char *schema, const char *table) {
	PostgresFunctionGuard(mooncake_scan_begin, schema, table, &data, &len);
}

MooncakeScanFfi::~MooncakeScanFfi() {
	PostgresFunctionGuard(mooncake_scan_end, data, len);
}

MooncakeTableMetadata
MooncakeScanFfi::GetTableMetadata() const {
	MooncakeTableMetadata metadata;
	uint32_t *ptr = reinterpret_cast<uint32_t *>(data);
	metadata.data_files_len = *ptr++;
	metadata.data_files_offsets = ptr;
	ptr += metadata.data_files_len + 1;
	metadata.position_deletes_len = *ptr++;
	metadata.position_deletes_data = ptr;
	metadata.data_files_data = reinterpret_cast<const char *>(ptr + 2 * metadata.position_deletes_len);
	return metadata;
}

} // namespace pgduckdb
