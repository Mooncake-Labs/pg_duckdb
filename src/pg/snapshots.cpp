#include "pgduckdb/pg/snapshots.hpp"

extern "C" {
#include "postgres.h"
#include "access/xlog.h"
}

namespace pgduckdb::pg {

uint64_t
GetActiveLsn() {
	// NOTE: XactLastCommitEnd refers to the last commit LSN for the current backend process, not the entire server
	return XactLastCommitEnd;
}

} // namespace pgduckdb::pg
