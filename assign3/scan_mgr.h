#ifndef SCAN_MGR_H
#define SCAN_MGR_H

#include "record_mgr.h"

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
RC next(RM_ScanHandle *scan, Record *record);
RC closeScan(RM_ScanHandle *scan);

#endif
