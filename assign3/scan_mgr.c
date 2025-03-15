#include "scan_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "expr.h"
#include "dberror.h"
#include "tables.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h> 

#define PAGE_METADATA_SIZE sizeof(int)
#define SLOT_SIZE 20

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    scan->rel = rel;
    scan->mgmtData = malloc(sizeof(int));
    *(int *)(scan->mgmtData) = 0;
    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {
    int *currentSlot = (int *)scan->mgmtData;
    BM_BufferPool *bm = (BM_BufferPool *)scan->rel->mgmtData;
    BM_PageHandle page;

    if (pinPage(bm, &page, 1) != RC_OK) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    int numRecords = getNumTuples(scan->rel);
    printf("Debug: Total Records: %d\n", numRecords);

    if (numRecords < 0) {
        printf("Warning: numRecords contains invalid data: %d. Resetting to 0.\n", numRecords);
        numRecords = 0;
    }

    while (*currentSlot < numRecords) {
        char *recordData = page.data + PAGE_METADATA_SIZE + (*currentSlot * SLOT_SIZE);

        if (recordData[0] != '\0') {
            memset(record->data, 0, SLOT_SIZE);
            strncpy(record->data, recordData, SLOT_SIZE - 1);
            record->data[SLOT_SIZE - 1] = '\0';

            printf("Debug: Scanned Record at Slot %d: %s\n", *currentSlot, record->data);

            (*currentSlot)++;
            unpinPage(bm, &page);
            return RC_OK;
        }

        (*currentSlot)++;
    }

    unpinPage(bm, &page);
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan(RM_ScanHandle *scan) {
    free(scan->mgmtData);
    return RC_OK;
}
