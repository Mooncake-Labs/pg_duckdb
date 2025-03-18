#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "pgduckdb/pg/declarations.hpp"
#include "pgduckdb/scan/mooncake_table_reader.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

class PostgresTable : public duckdb::TableCatalogEntry {
public:
	PostgresTable(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	              Relation rel, Cardinality cardinality, Snapshot snapshot);

	virtual ~PostgresTable();

public:
	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                         duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

public:
	static Relation OpenRelation(Oid relid);
	static void SetTableInfo(duckdb::CreateTableInfo &info, Relation rel);

protected:
	Relation rel;
	Cardinality cardinality;
	Snapshot snapshot;
};

class MooncakeTable : public PostgresTable {
public:
	MooncakeTable(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	              Relation rel, Cardinality cardinality, Snapshot snapshot, uint32_t table_id, uint64_t lsn);

public:
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;

private:
	MooncakeTableReader reader;
};

} // namespace pgduckdb
