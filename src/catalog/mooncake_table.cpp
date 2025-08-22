#include "pgduckdb/catalog/mooncake_table.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" uint64_t mooncake_get_table_cardinality(const char *schema, const char *table);

extern "C" void mooncake_scan_table_begin(const char *schema, const char *table, uint64_t lsn, uint8_t **data,
                                          size_t *len);

extern "C" void mooncake_scan_table_end(const char *schema, const char *table, uint8_t *data, size_t len);

namespace pgduckdb {

MooncakeTable::MooncakeTable(duckdb::Catalog &_catalog, duckdb::SchemaCatalogEntry &_schema,
                             duckdb::CreateTableInfo &info, Relation _rel, Cardinality _cardinality, Snapshot _snapshot,
                             uint64_t _lsn)
    : PostgresTable(_catalog, _schema, info, _rel, _cardinality, _snapshot), lsn(_lsn), cardinality(0), lock(),
      data(nullptr), len(0), metadata() {
	cardinality = PostgresFunctionGuard(mooncake_get_table_cardinality, schema.name.c_str(), name.c_str());
}

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
