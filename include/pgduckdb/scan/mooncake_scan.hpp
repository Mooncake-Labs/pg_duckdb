#pragma once

#include "duckdb/function/table_function.hpp"

namespace pgduckdb {

class MooncakeTableReader;

duckdb::TableFunction GetMooncakeScanFunction(duckdb::ClientContext &context, duckdb::TableCatalogEntry &table,
                                              MooncakeTableReader &reader, duckdb::idx_t cardinality,
                                              duckdb::unique_ptr<duckdb::FunctionData> &bind_data);

} // namespace pgduckdb
