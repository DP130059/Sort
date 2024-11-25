#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <memory.h>

#define KEY_SIZE 8
#define REC_SIZE 56

typedef struct {
  unsigned long key;
  unsigned char record[REC_SIZE];
} tuple;

int main(int argc, char **argv) {
  char fileAddr[128] = "./Tuple";
  unsigned long DATA_SIZE = 1ul;
  if (argc < 1) {
    printf("Too few Arguments!\n");
    exit(-1);
  } else {
    DATA_SIZE = atol(argv[1]);
  }
  printf("%ld\n", DATA_SIZE);
  unsigned long BUF_SIZE = 10ul;
  if (DATA_SIZE < 10ul) {
    BUF_SIZE = 1ul;
  }
  printf("%ld\n", BUF_SIZE);
  unsigned long HOW_MANY_BUF = DATA_SIZE / BUF_SIZE, BUF_NUMBERS = BUF_SIZE * (1ul << 24l);
  strcat(fileAddr, argv[1]);
  printf("%ld\n", HOW_MANY_BUF);
  printf("%ld\n", BUF_NUMBERS);
  FILE *dataFile = fopen(fileAddr, "wb");
  tuple *BUF = (tuple *) malloc(sizeof(tuple) * BUF_NUMBERS);
  memset(BUF, 0, sizeof(tuple) * BUF_NUMBERS);
  srand((unsigned) time(NULL));
  for (unsigned long i = 0; i < HOW_MANY_BUF; i++) {
    for (unsigned long j = 0; j < BUF_NUMBERS; j++) {
      tuple *tuplePtr = BUF + j;
      unsigned *unsignedPtr = (unsigned *) tuplePtr;
      for (unsigned long k = 0; k < (KEY_SIZE + REC_SIZE) >> 2; k++) {
        *(unsignedPtr + k) = (unsigned) (rand() << 1);
      }
      tuplePtr = NULL;
      unsignedPtr = NULL;
    }
    fwrite(BUF, sizeof(tuple), BUF_NUMBERS, dataFile);
  }
  free(BUF);
  BUF = NULL;
  return 0;
}