#include "pgduckdb/catalog/mooncake_table.hpp"
#include "pgduckdb/scan/mooncake_scan.hpp"

namespace pgduckdb {

MooncakeTable::MooncakeTable(duckdb::Catalog &_catalog, duckdb::SchemaCatalogEntry &_schema,
                             duckdb::CreateTableInfo &info, Relation _rel, Cardinality _cardinality, Snapshot _snapshot,
                             uint32_t table_id, uint64_t lsn)
    : PostgresTable(_catalog, _schema, info, _rel, _cardinality, _snapshot), reader(table_id, lsn) {
}

duckdb::TableFunction
MooncakeTable::GetScanFunction(duckdb::ClientContext &context, duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	return GetMooncakeScanFunction(context, *this, reader, cardinality, bind_data);
}

} // namespace pgduckdb
