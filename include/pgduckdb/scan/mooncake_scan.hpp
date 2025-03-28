#pragma once

#include "duckdb/function/table_function.hpp"

namespace pgduckdb {

duckdb::TableFunction GetMooncakeScanFunction(duckdb::ClientContext &context, duckdb::TableCatalogEntry &table,
                                              duckdb::idx_t cardinality,
                                              duckdb::unique_ptr<duckdb::FunctionData> &bind_data);

} // namespace pgduckdb
