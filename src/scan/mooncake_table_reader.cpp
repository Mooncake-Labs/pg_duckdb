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

MooncakeTableMetadata
MooncakeTableReader::LazyGetTableMetadata() {
	duckdb::lock_guard<duckdb::mutex> guard(lock);
	if (!data) {
		PostgresFunctionGuard(mooncake_scan_table_begin, table_id, lsn, &data, &len);
		uint32_t *ptr = reinterpret_cast<uint32_t *>(data);

		metadata.data_files_len = *ptr++;
		metadata.data_files_offsets = ptr;
		ptr += metadata.data_files_len + 1;

		metadata.deletion_vectors_len = *ptr++;
		metadata.deletion_vectors_offsets = ptr;
		ptr += metadata.deletion_vectors_len + 1;
		metadata.deletion_vectors_blobs = ptr;
		ptr += 3 * metadata.deletion_vectors_len;

		metadata.position_deletes_len = *ptr++;
		metadata.position_deletes_data = ptr;
		ptr += 2 * metadata.position_deletes_len;

		char *char_ptr = reinterpret_cast<char *>(ptr);
		metadata.data_files_data = char_ptr;
		char_ptr += metadata.data_files_offsets[metadata.data_files_len];
		metadata.deletion_vectors_data = char_ptr;
		char_ptr += metadata.deletion_vectors_offsets[metadata.deletion_vectors_len];
		D_ASSERT(char_ptr == reinterpret_cast<char *>(data + len));
	}
	return metadata;
}

} // namespace pgduckdb
