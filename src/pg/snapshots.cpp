extern "C" {
#include "postgres.h"
#include "access/xlog.h"
}

extern "C" uint64_t
GetActiveLsn() {
	// NOTE: Approximate using last commit record end from the current backend
	return XactLastCommitEnd;
}
