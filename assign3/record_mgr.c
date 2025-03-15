#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "tables.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_METADATA_SIZE sizeof(int)
#define SLOT_SIZE 20

RC initRecordManager(void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}

RC createTable(char *name, Schema *schema) {
    SM_FileHandle fHandle;
    if (createPageFile(name) != RC_OK) return RC_FILE_NOT_FOUND;
    if (openPageFile(name, &fHandle) != RC_OK) return RC_FILE_HANDLE_NOT_INIT;
    
    char *pageData = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (!pageData) return RC_MEMORY_ALLOCATION_ERROR;
    
    int numRecords = 0;
    memcpy(pageData, &numRecords, sizeof(int));

    if (writeBlock(0, &fHandle, pageData) != RC_OK) {
        free(pageData);
        return RC_WRITE_FAILED;
    }

    free(pageData);
    closePageFile(&fHandle);
    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name) {
    rel->name = name;
    rel->mgmtData = malloc(sizeof(BM_BufferPool));
    initBufferPool((BM_BufferPool *)rel->mgmtData, name, 3, RS_FIFO, NULL);
    return RC_OK;
}

RC closeTable(RM_TableData *rel) {
    shutdownBufferPool((BM_BufferPool *)rel->mgmtData);
    free(rel->mgmtData);
    return RC_OK;
}

RC deleteTable(char *name) {
    return destroyPageFile(name);
}

int getNumTuples(RM_TableData *rel) {
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle page;

    if (pinPage(bm, &page, 0) != RC_OK) {
        return -1;
    }

    int numTuples = 0;
    memcpy(&numTuples, page.data, sizeof(int));

    if (numTuples < 0 || numTuples > 1000000) {
        printf("Warning: numTuples is corrupted: %d. Resetting to 0.\n", numTuples);
        numTuples = 0;
        memcpy(page.data, &numTuples, sizeof(int));
        markDirty(bm, &page);
        forcePage(bm, &page);
    }

    printf("Debug: Read numTuples from metadata: %d\n", numTuples);

    unpinPage(bm, &page);
    return numTuples;
}

RC insertRecord(RM_TableData *rel, Record *record) {
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle page;
    int numTuples;

    if (pinPage(bm, &page, 0) != RC_OK) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    memcpy(&numTuples, page.data, sizeof(int));
    if (numTuples < 0) {
        printf("Warning: numTuples is corrupted, resetting to 0.\n");
        numTuples = 0;
    }

    printf("Debug: Before Insertion, Num Tuples: %d\n", numTuples);

    int pageNum = 1 + (numTuples / (PAGE_SIZE / SLOT_SIZE)); 
    int slot = numTuples % (PAGE_SIZE / SLOT_SIZE); 

    record->id.page = pageNum;
    record->id.slot = slot;

    if (pinPage(bm, &page, pageNum) != RC_OK) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    memset(page.data + PAGE_METADATA_SIZE + (slot * SLOT_SIZE), 0, SLOT_SIZE);

    strncpy(page.data + PAGE_METADATA_SIZE + (slot * SLOT_SIZE), record->data, SLOT_SIZE - 1);
    page.data[PAGE_METADATA_SIZE + (slot * SLOT_SIZE) + SLOT_SIZE - 1] = '\0'; 

    markDirty(bm, &page);
    forcePage(bm, &page);
    unpinPage(bm, &page);

    if (pinPage(bm, &page, 0) != RC_OK) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    numTuples++;
    memcpy(page.data, &numTuples, sizeof(int));

    markDirty(bm, &page);
    forcePage(bm, &page);
    unpinPage(bm, &page);

    printf("Debug: After Insertion, Num Tuples: %d\n", numTuples);
    printf("Debug: Record inserted at Page: %d, Slot: %d with Data: %s\n", record->id.page, record->id.slot, record->data);

    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle page;

    if (pinPage(bm, &page, 0) != RC_OK) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    int numTuples;
    memcpy(&numTuples, page.data, sizeof(int));

    printf("Debug: Before Deletion, Num Tuples: %d\n", numTuples);

    if (numTuples > 0) {
        numTuples--;
    } else {
        printf("Warning: Trying to delete from an empty table.\n");
    }

    memcpy(page.data, &numTuples, sizeof(int));
    markDirty(bm, &page);
    forcePage(bm, &page);
    unpinPage(bm, &page);

    if (pinPage(bm, &page, id.page) != RC_OK) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    page.data[PAGE_METADATA_SIZE + (id.slot * SLOT_SIZE)] = '\0';
    markDirty(bm, &page);
    unpinPage(bm, &page);

    printf("Debug: After Deletion, Num Tuples: %d\n", numTuples);
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle page;

    if (pinPage(bm, &page, record->id.page) != RC_OK) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    memset(page.data + PAGE_METADATA_SIZE + (record->id.slot * SLOT_SIZE), 0, SLOT_SIZE);

    strncpy(page.data + PAGE_METADATA_SIZE + (record->id.slot * SLOT_SIZE), record->data, SLOT_SIZE - 1);
    page.data[PAGE_METADATA_SIZE + (record->id.slot * SLOT_SIZE) + SLOT_SIZE - 1] = '\0';

    printf("Debug: Updated record at Page: %d, Slot: %d with Data: '%s'\n", record->id.page, record->id.slot, record->data);

    markDirty(bm, &page);
    forcePage(bm, &page);
    unpinPage(bm, &page);

    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    BM_BufferPool *bm = (BM_BufferPool *)rel->mgmtData;
    BM_PageHandle page;

    if (pinPage(bm, &page, id.page) != RC_OK) {
        printf("Debug: Failed to pin page %d\n", id.page);
        return RC_READ_NON_EXISTING_PAGE;
    }

    if (record->data == NULL) {
        record->data = (char *)malloc(SLOT_SIZE);
        if (record->data == NULL) {
            printf("Debug: Memory allocation failed for record->data\n");
            return RC_MEMORY_ALLOCATION_ERROR;
        }
    }

    char *recordData = page.data + PAGE_METADATA_SIZE + (id.slot * SLOT_SIZE);
    strncpy(record->data, recordData, SLOT_SIZE - 1);
    record->data[SLOT_SIZE - 1] = '\0';  

    printf("Debug: Retrieved Record from Page: %d, Slot: %d -> %s\n", id.page, id.slot, record->data);

    unpinPage(bm, &page);
    return RC_OK;
}

int getRecordSize(Schema *schema) {
    return SLOT_SIZE;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    return schema;
}

RC freeSchema(Schema *schema) {
    free(schema);
    return RC_OK;
}

RC createRecord(Record **record, Schema *schema) {
    *record = (Record *)malloc(sizeof(Record));
    if (*record == NULL) return RC_MEMORY_ALLOCATION_ERROR;

    (*record)->data = (char *)malloc(getRecordSize(schema));
    if ((*record)->data == NULL) {
        free(*record);
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    memset((*record)->data, 0, getRecordSize(schema));
    return RC_OK;
}

RC freeRecord(Record *record) {
    free(record->data);
    free(record);
    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    if (record == NULL || schema == NULL || value == NULL) return RC_ERROR;

    *value = (Value *)malloc(sizeof(Value));
    if (*value == NULL) return RC_MEMORY_ALLOCATION_ERROR;

    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            (*value)->dt = DT_INT;
            memcpy(&((*value)->v.intV), record->data + (attrNum * sizeof(int)), sizeof(int));
            break;
        case DT_STRING:
            (*value)->dt = DT_STRING;
            (*value)->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
            strncpy((*value)->v.stringV, record->data + (attrNum * schema->typeLength[attrNum]), schema->typeLength[attrNum]);
            (*value)->v.stringV[schema->typeLength[attrNum]] = '\0';
            break;
        case DT_FLOAT:
            (*value)->dt = DT_FLOAT;
            memcpy(&((*value)->v.floatV), record->data + (attrNum * sizeof(float)), sizeof(float));
            break;
        case DT_BOOL:
            (*value)->dt = DT_BOOL;
            memcpy(&((*value)->v.boolV), record->data + (attrNum * sizeof(bool)), sizeof(bool));
            break;
        default:
            free(*value);
            return RC_ERROR;
    }

    return RC_OK;
}

int main() {
    printf("Initializing Record Manager...\n");
    initRecordManager(NULL);
    printf("Record Manager initialized.\n");

    char *attrNames[] = {"ID", "Name", "Age"};
    DataType dataTypes[] = {DT_INT, DT_STRING, DT_INT};
    int typeLength[] = {sizeof(int), 10, sizeof(int)};
    int keys[] = {0};

    printf("Creating table 'test_table'...\n");
    Schema *schema = createSchema(3, attrNames, dataTypes, typeLength, 1, keys);
    if (createTable("test_table", schema) != RC_OK) {
        printf("Error creating table.\n");
        return 1;
    }
    printf("Table 'test_table' created successfully.\n");

    RM_TableData table;
    if (openTable(&table, "test_table") != RC_OK) {
        printf("Error opening table.\n");
        return 1;
    }
    printf("Table 'test_table' opened successfully.\n");

    printf("Testing Insert and Retrieve...\n");
    Record *record;
    if (createRecord(&record, schema) != RC_OK) {
        printf("Error creating record.\n");
        return 1;
    }
    strncpy(record->data, "test_data", SLOT_SIZE - 1);
    record->data[SLOT_SIZE - 1] = '\0';

    printf("Debug: Before Insertion, Num Tuples: %d\n", getNumTuples(&table));

    if (insertRecord(&table, record) != RC_OK) {
        printf("Error inserting record.\n");
        return 1;
    }
    printf("Debug: Record inserted at Page: %d, Slot: %d\n", record->id.page, record->id.slot);
    printf("Debug: After Insertion, Num Tuples: %d\n", getNumTuples(&table));

    Record *retrieved;
    if (createRecord(&retrieved, schema) != RC_OK) {
        printf("Error creating retrieved record.\n");
        return 1;
    }

    if (getRecord(&table, record->id, retrieved) != RC_OK) {
        printf("Error retrieving record.\n");
        return 1;
    }

    printf("Inserted Data: %s\n", record->data);
    printf("Retrieved Data: %s\n", retrieved->data);

    printf("Testing Delete...\n");
    printf("Debug: Before Deletion, Num Tuples: %d\n", getNumTuples(&table));

    if (deleteRecord(&table, record->id) != RC_OK) {
        printf("Delete failed!\n");
        return 1;
    }
    printf("Debug: Record deleted at Page: %d, Slot: %d\n", record->id.page, record->id.slot);
    printf("Debug: After Deletion, Num Tuples: %d\n", getNumTuples(&table));

    printf("Testing Update...\n");
    strncpy(record->data, "updated_data", SLOT_SIZE - 1);
    record->data[SLOT_SIZE - 1] = '\0';

    printf("Debug: Updating record at Page: %d, Slot: %d with Data: %s\n", record->id.page, record->id.slot, record->data);

    if (updateRecord(&table, record) != RC_OK) {
        printf("Update failed!\n");
        return 1;
    }
    printf("Record updated successfully.\n");

    Record *updatedRecord;
    if (createRecord(&updatedRecord, schema) != RC_OK) {
        printf("Error creating updated record.\n");
        return 1;
    }

    if (getRecord(&table, record->id, updatedRecord) != RC_OK) {
        printf("Retrieve after update failed!\n");
        return 1;
    }
    printf("Updated Data: %s\n", updatedRecord->data);

    printf("Testing Scan...\n");
    RM_ScanHandle scan;
    if (startScan(&table, &scan, NULL) != RC_OK) {
        printf("Error starting scan.\n");
        return 1;
    }

    Record *scannedRecord;
    if (createRecord(&scannedRecord, schema) != RC_OK) {
        printf("Error creating scan record.\n");
        return 1;
    }

    while (next(&scan, scannedRecord) != RC_RM_NO_MORE_TUPLES) {
        printf("Scanned Record: %s\n", scannedRecord->data);
    }
    printf("Scan completed.\n");

    closeScan(&scan);
    freeRecord(scannedRecord);
    
    freeRecord(record);
    freeRecord(retrieved);
    freeRecord(updatedRecord);
    closeTable(&table);
    deleteTable("test_table");
    shutdownRecordManager();

    printf("Record Manager shutdown complete.\n");
    return 0;
}
