#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct PageFrame {
    BM_PageHandle page;
    bool dirty;
    int fixCount;
    struct PageFrame *next;
} PageFrame;

typedef struct BM_MgmtData {
    PageFrame *pageFrames;
    int numReadIO;
    int numWriteIO;
} BM_MgmtData;

RC initBufferPool(BM_BufferPool *bm, const char *pageFileName, int numPages, ReplacementStrategy strategy, void *stratData) {
    bm->pageFile = (char *)malloc(strlen(pageFileName) + 1);
    strcpy(bm->pageFile, pageFileName);
    bm->numPages = numPages;
    bm->strategy = strategy;
    
    BM_MgmtData *mgmtData = (BM_MgmtData *)malloc(sizeof(BM_MgmtData));
    mgmtData->pageFrames = (PageFrame *)calloc(numPages, sizeof(PageFrame));
    mgmtData->numReadIO = 0;
    mgmtData->numWriteIO = 0;
    bm->mgmtData = mgmtData;
    
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *bm) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
    free(mgmtData->pageFrames);
    free(mgmtData);
    free(bm->pageFile);
    return RC_OK;
}

RC pinPage(BM_BufferPool *bm, BM_PageHandle *page, PageNumber pageNum) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
    mgmtData->numReadIO++;
    
    page->pageNum = pageNum;
    page->data = (char *)malloc(PAGE_SIZE);
    
    SM_FileHandle fileHandle;
    if (openPageFile(bm->pageFile, &fileHandle) != RC_OK)
        return RC_FILE_NOT_FOUND;
    
    if (readBlock(pageNum, &fileHandle, page->data) != RC_OK)
        return RC_READ_NON_EXISTING_PAGE;

    closePageFile(&fileHandle);
    return RC_OK;
}

RC unpinPage(BM_BufferPool *bm, BM_PageHandle *page) {
    free(page->data);
    return RC_OK;
}

RC markDirty(BM_BufferPool *bm, BM_PageHandle *page) {
    return RC_OK;
}

RC forcePage(BM_BufferPool *bm, BM_PageHandle *page) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
    mgmtData->numWriteIO++;
    
    SM_FileHandle fileHandle;
    if (openPageFile(bm->pageFile, &fileHandle) != RC_OK)
        return RC_FILE_NOT_FOUND;
    
    if (writeBlock(page->pageNum, &fileHandle, page->data) != RC_OK)
        return RC_WRITE_FAILED;
    
    closePageFile(&fileHandle);
    return RC_OK;
}

PageNumber *getFrameContents(BM_BufferPool *bm) {
    PageNumber *contents = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++)
        contents[i] = NO_PAGE;
    return contents;
}

bool *getDirtyFlags(BM_BufferPool *bm) {
    bool *dirtyFlags = (bool *)malloc(sizeof(bool) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++)
        dirtyFlags[i] = false;
    return dirtyFlags;
}

int *getFixCounts(BM_BufferPool *bm) {
    int *fixCounts = (int *)malloc(sizeof(int) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++)
        fixCounts[i] = 0;
    return fixCounts;
}

RC forceFlushPool(BM_BufferPool *bm) {
    return RC_OK;
}
