#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void initStorageManager() {
} 

RC createPageFile(char *fileName) {
    FILE *file = fopen(fileName, "w");
    if (!file) return RC_FILE_NOT_FOUND;
    
    char emptyPage[PAGE_SIZE] = {0};
    fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    fclose(file);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *file = fopen(fileName, "r+");
    if (!file) return RC_FILE_NOT_FOUND;
    
    fHandle->fileName = fileName;
    fHandle->totalNumPages = 1;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = file;
    
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    return fclose((FILE *)fHandle->mgmtInfo) == 0 ? RC_OK : RC_FILE_HANDLE_NOT_INIT;
}

RC destroyPageFile(char *fileName) {
    return remove(fileName) == 0 ? RC_OK : RC_FILE_NOT_FOUND;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, char *memPage) {
    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (fseek(file, pageNum * PAGE_SIZE, SEEK_SET) != 0) return RC_READ_NON_EXISTING_PAGE;
    fread(memPage, sizeof(char), PAGE_SIZE, file);
    return RC_OK;
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, char *memPage) {
    FILE *file = (FILE *)fHandle->mgmtInfo;
    if (fseek(file, pageNum * PAGE_SIZE, SEEK_SET) != 0) return RC_WRITE_FAILED;
    fwrite(memPage, sizeof(char), PAGE_SIZE, file);
    return RC_OK;
}