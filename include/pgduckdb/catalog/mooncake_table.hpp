#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "pgduckdb/scan/mooncake_table_reader.hpp"

namespace pgduckdb {

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
