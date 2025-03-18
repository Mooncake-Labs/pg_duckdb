#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "pgduckdb/catalog/mooncake_table_metadata.hpp"
#include "pgduckdb/vendor/roaring.hpp"

using namespace duckdb;
using namespace roaring;

namespace pgduckdb {

struct MooncakeTableMetadata::DeletionVector {
	ClientContext &context;
	string_t puffin_file;
	uint32_t offset;
	uint32_t size;

	mutex lock;
	bool initialized;
	Roaring map;

	DeletionVector(ClientContext &_context, string_t _puffin_file, uint32_t _offset, uint32_t _size)
	    : context(_context), puffin_file(_puffin_file), offset(_offset), size(_size), initialized(false) {
	}

	void
	LazyInitialize() {
		lock_guard<mutex> guard(lock);
		if (!initialized) {
			auto &fs = FileSystem::GetFileSystem(context);
			auto file_handle = fs.OpenFile(string(puffin_file), FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ));
			char buffer[1024];
			// | 4-byte length | 4-byte magic | buffer | CRC-32 |
			file_handle->Read(buffer, size - 12, offset + 8);
			// TODO
			// | 8-byte #keys |
			// | 4-byte key | map |
			D_ASSERT(*reinterpret_cast<uint64_t *>(buffer) == 1);
			D_ASSERT(*reinterpret_cast<uint32_t *>(buffer + 8) == 0);
			map = Roaring::read(buffer + 12);
		}
	}
};

MooncakeTableMetadata::MooncakeTableMetadata()
    : data_files_len(0), data_files_offsets(nullptr), data_files_data(nullptr) {
}

MooncakeTableMetadata::~MooncakeTableMetadata() = default;

void
MooncakeTableMetadata::Initialize(ClientContext &context, uint8_t *data, size_t len) {
	uint32_t *ptr = reinterpret_cast<uint32_t *>(data);

	data_files_len = *ptr++;
	data_files_offsets = ptr;
	ptr += data_files_len + 1;

	uint32_t puffin_files_len = *ptr++;
	const uint32_t *puffin_files_offsets = ptr;
	ptr += puffin_files_len + 1;

	uint32_t deletion_vectors_len = *ptr++;
	const uint32_t *deletion_vectors_data = ptr;
	ptr += 4 * deletion_vectors_len;

	uint32_t position_deletes_len = *ptr++;
	const uint32_t *position_deletes_data = ptr;
	ptr += 2 * position_deletes_len;

	char *char_ptr = reinterpret_cast<char *>(ptr);
	data_files_data = char_ptr;
	char_ptr += data_files_offsets[data_files_len];
	const char *puffin_files_data = char_ptr;
	char_ptr += puffin_files_offsets[puffin_files_len];
	D_ASSERT(char_ptr == reinterpret_cast<char *>(data + len));

	data_file_deletions.resize(data_files_len);
	for (idx_t i = 0; i < deletion_vectors_len; i++) {
		uint32_t data_file_number = deletion_vectors_data[4 * i];
		uint32_t puffin_file_number = deletion_vectors_data[4 * i + 1];
		uint32_t offset = deletion_vectors_data[4 * i + 2];
		uint32_t size = deletion_vectors_data[4 * i + 3];
		string_t puffin_file(puffin_files_data + puffin_files_offsets[puffin_file_number],
		                     puffin_files_offsets[puffin_file_number + 1] - puffin_files_offsets[puffin_file_number]);
		data_file_deletions[data_file_number].first = make_uniq<DeletionVector>(context, puffin_file, offset, size);
	}
	for (idx_t i = 0; i < position_deletes_len; i++) {
		uint32_t data_file_number = position_deletes_data[2 * i];
		uint32_t data_file_row_number = position_deletes_data[2 * i + 1];
		data_file_deletions[data_file_number].second = true;
		position_deletes.insert({data_file_number, data_file_row_number});
	}
}

bool
MooncakeTableMetadata::HasDeletes(uint32_t data_file_number) {
	auto &[deletion_vector, has_position_deletes] = data_file_deletions[data_file_number];
	if (deletion_vector) {
		deletion_vector->LazyInitialize();
	}
	return deletion_vector || has_position_deletes;
}

bool
MooncakeTableMetadata::IsDeleted(uint32_t data_file_number, uint32_t data_file_row_number) {
	auto &[deletion_vector, has_position_deletes] = data_file_deletions[data_file_number];
	return (deletion_vector && deletion_vector->map.contains(data_file_row_number)) ||
	       (has_position_deletes && position_deletes.count({data_file_number, data_file_row_number}));
}

} // namespace pgduckdb
