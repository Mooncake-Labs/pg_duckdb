#pragma once

#include "pgduckdb/pg/declarations.hpp"

extern "C" {
extern Snapshot GetActiveSnapshot(void);
}

namespace pgduckdb::pg {
uint64_t GetActiveLsn();
}
