#include "pgduckdb/scan/mooncake_table_metadata.hpp"

extern "C" char *get_mooncake_table_metadata();

namespace pgduckdb {

MooncakeTableMetadata
GetMooncakeTableMetadata() {
	uint32_t *buffer = reinterpret_cast<uint32_t *>(get_mooncake_table_metadata());
	MooncakeTableMetadata metadata;
	metadata.data_files.len = *buffer++;
	metadata.data_files.offsets = buffer;
	buffer += metadata.data_files.len + 1;
	metadata.position_deletes.len = *buffer++;
	metadata.position_deletes.data = buffer;
	metadata.data_files.data = reinterpret_cast<char *>(buffer + 2 * metadata.position_deletes.len);
	return metadata;
}

} // namespace pgduckdb
