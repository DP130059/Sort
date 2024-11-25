#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

#define REC_SIZE 56

unsigned long DATA_SIZE = 0ul, BUF_SIZE = 0ul;
typedef struct {
  unsigned long key;
  unsigned char record[REC_SIZE];
} tuple;

char check(tuple *BUF) {
  for (unsigned long i = 0; i < BUF_SIZE - 1; i++) {
    if (BUF[i].key > BUF[i + 1].key) {
      return -1;
    }
  }
  return 1;
}

int main(int argc, char **argv) {
  char fileAddr[128] = "./SortedTuple";
  DATA_SIZE = atol(argv[1]);
  BUF_SIZE = DATA_SIZE << 24ul;
  strcat(fileAddr, argv[1]);
  tuple *BUF = (tuple *) malloc(sizeof(tuple) * BUF_SIZE);
  FILE *dataFile = fopen(fileAddr, "rb");
  fread(BUF, sizeof(tuple), BUF_SIZE, dataFile);
  char result = check(BUF);
  if (result > 0)
    printf("Success!\n");
  else
    printf("Fail!\n");
  fclose(dataFile);
  free(BUF);
  BUF = NULL;
  return 0;
}

//
// Created by DP on 2024/11/25.
//
