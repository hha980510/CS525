README: Simple Storage Manager

-Overview-

This project is a Simple Storage Manager that lets you manage files on disk. It allows creating, opening, reading, writing, and handling fixed-size blocks (pages) of data. It’s a basic system that could be the starting point for building a database.

-Features-

File Management:

Create a new file with an empty page.

Open and read metadata of an existing file.

Close or delete a file.

Block Handling:

Read and write pages at specific or relative positions (first, last, next, etc.).

Append empty pages and ensure a minimum number of pages.

Error Handling:

Returns meaningful error codes for invalid actions like reading non-existent pages.

-Project Files-

Source Code:

storage_mgr.c: Core implementation of the storage manager.

dberror.c: Handles error codes and messages.

test_assign1_1.c: Contains test cases.

test_helper.h: Provides helper functions for testing.

Headers:

storage_mgr.h: Declares storage manager functions.

dberror.h: Defines error codes.

Makefile: Automates building and cleaning the project.

-How to Use-

Build the Project:

make

Run Tests:

./test_assign1

Clean Build Files:

make clean

-Design Highlights-

File Handle (SM_FileHandle):

Keeps file metadata: name, total pages, current position, and management info.

Page Handle (SM_PageHandle):

A buffer for storing or reading page data.

Implementation:

Uses standard C functions for file operations (fopen, fwrite, fseek, etc.).

Handles dynamic memory allocation (calloc/free) safely.

-Testing-

The provided tests validate:

Reading and writing to valid and invalid blocks.

Ensuring capacity by appending pages.

Proper handling of errors like file not found or invalid reads.

-Assumptions and Future Work-

Current Limitations:

Fixed page size of 4096 bytes (PAGE_SIZE).

Platform-specific behaviors (e.g., permissions) are not covered.

Potential Enhancements:

Support variable page sizes.

Add concurrency for multi-threaded environments.

Improve error messages for debugging.

-Copy Right-

Hyunsung Ha

Created for CS512: Advanced Database Organization.

