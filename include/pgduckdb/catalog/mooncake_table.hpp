#pragma once

#include "pgduckdb/catalog/mooncake_table_metadata.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"

namespace pgduckdb {

class MooncakeTable : public PostgresTable {
public:
	MooncakeTable(duckdb::Catalog &_catalog, duckdb::SchemaCatalogEntry &_schema, duckdb::CreateTableInfo &info,
	              Relation _rel, Cardinality _cardinality, Snapshot _snapshot, uint64_t _lsn)
	    : PostgresTable(_catalog, _schema, info, _rel, _cardinality, _snapshot), lsn(_lsn), lock(), data(nullptr),
	      len(0), metadata() {
	}

	~MooncakeTable();

	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;

	MooncakeTableMetadata &GetTableMetadata();

private:
	MooncakeTable(const MooncakeTable &) = delete;
	MooncakeTable &operator=(const MooncakeTable &) = delete;

	uint64_t lsn;

	duckdb::mutex lock;
	uint8_t *data;
	size_t len;
	duckdb::unique_ptr<MooncakeTableMetadata> metadata;
};

} // namespace pgduckdb
