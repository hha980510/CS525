# Compiler to use
CC = gcc

# Compiler flags
CFLAGS = -Wall -g

# The target program
TARGET = test_assign1

# Object files
OBJS = storage_mgr.o dberror.o test_assign1_1.o

# Rule to build the final executable
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

# Rule to compile storage_mgr.c
storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
	$(CC) $(CFLAGS) -c storage_mgr.c

# Rule to compile dberror.c
dberror.o: dberror.c dberror.h
	$(CC) $(CFLAGS) -c dberror.c

# Rule to compile test_assign1_1.c
test_assign1_1.o: test_assign1_1.c storage_mgr.h dberror.h test_helper.h
	$(CC) $(CFLAGS) -c test_assign1_1.c

# Clean rule to remove intermediate and final files
clean:
	rm -f $(OBJS) $(TARGET)