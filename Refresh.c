#include<stdlib.h>
#include<string.h>
#include<stdio.h>
#include<mpi.h>
#include<time.h>
#include<sys/time.h>
#include<memory.h>

typedef struct {
  unsigned long key;
  char record[REC_SIZE];
} tuple;
