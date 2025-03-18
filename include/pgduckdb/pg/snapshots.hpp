#pragma once

extern "C" {
extern Snapshot GetActiveSnapshot(void);

uint64_t GetActiveLsn();
}
