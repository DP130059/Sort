#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
//#include <mpi.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
//#include "/usr/include/x86_64-linux-gnu/sys/time.h"
#include <sys/time.h>
#define KEY_SIZE 8l
#define REC_SIZE 56l
#define BUCKET_SIZE 1024ul
typedef struct {
  unsigned long key;
  unsigned char record[REC_SIZE];
} tuple;

unsigned long DATA_SIZE = 0ul, BUF_SIZE = 0ul, BUF_NUMBER = 0ul, HOW_MANY_BUF = 0ul;

void printArray(tuple *BUF, unsigned long size) {
  for (unsigned long i = 0; i < size; i++)
    printf("0x%lx,", BUF[i].key);
  printf("\n");
}
char cmp(tuple *t1, tuple *t2) {
  if (t1->key < t2->key) {
    return 1;
  } else {
    return -1;
  }
}

void swap(tuple *t1, tuple *t2) {
  tuple *tmp = (tuple *) malloc(sizeof(tuple));
  memset(tmp, 0, sizeof(tuple));
  tmp->key = t1->key;
  t1->key = t2->key;
  t2->key = tmp->key;
  for (char i = 0; i < REC_SIZE; i = i + 1) {
    tmp->record[i] = t1->record[i];
    t1->record[i] = t2->record[i];
    t2->record[i] = tmp->record[i];
  }
  free(tmp);
}

void swap2(tuple *t1, tuple *t2) {
  tuple tmp;
  tmp.key = t1->key;
  t1->key = t2->key;
  t2->key = tmp.key;
  memcpy(&tmp.record, t1->record, sizeof(char) * REC_SIZE);
  memcpy(t1->record, t2->record, sizeof(char) * REC_SIZE);
  memcpy(t2->record, &tmp.record, sizeof(char) * REC_SIZE);
}

void swap3(tuple *t1, tuple *t2) {
  tuple tmp;
  memcpy(&tmp.key,t1->record,sizeof(unsigned long));
  memcpy(&t1->key,&t2->key,sizeof(unsigned long));
  memcpy(&t2->key,&tmp.key,sizeof(unsigned long));
  memcpy(&tmp.record, t1->record, sizeof(char) * REC_SIZE);
  memcpy(t1->record, t2->record, sizeof(char) * REC_SIZE);
  memcpy(t2->record, &tmp.record, sizeof(char) * REC_SIZE);
}

void bubbleSort(tuple *BUF) {
  for (unsigned long i = 0; i < BUF_NUMBER - 1; i++) {
    for (unsigned long j = 0; j < BUF_NUMBER - i - 1; j++) {
      if (cmp(BUF + j, BUF + j + 1) > 0) {
        swap2(BUF + j, BUF + j + 1);
      }
    }
  }
}
void partition(tuple *BUF, unsigned long low, unsigned long high, unsigned long *piv) {
  unsigned long pivot = BUF[high].key;
  unsigned long i = low - 1;
  /*tuple *tmp=(tuple *)malloc(sizeof(tuple));
  memset(tmp,0,sizeof(tuple));*/
  for (unsigned long j = low; j < high; j++) {
    if (BUF[j].key < pivot) {
      i++;
      swap3(BUF + i, BUF + j);
    }
  }
  swap3(BUF + (i + 1), BUF + high);
  *piv = i + 1;
}
void fastSort(tuple *BUF, unsigned long low, unsigned high) {
  /*unsigned long *stack=(unsigned long *)malloc(sizeof(unsigned long)*BUF_NUMBER);
  memset(stack,0,sizeof(unsigned long)*BUF_NUMBER);*/
  unsigned long stack[1024];
  unsigned long top = 0ul, piv = 0ull;
  stack[top++] = low;
  stack[top++] = high;
  //printf("before\n");
  while (top > 0) {
    high = stack[--top];
    low = stack[--top];
    partition(BUF, low, high, &piv);
    //printf("start is %lu,piv is %lu,end is %lu\n",start,piv,end);
    if (piv > low) {
      stack[top++] = low;
      stack[top++] = piv - 1;
    }
    if (piv < high) {
      stack[top++] = piv + 1;
      stack[top++] = high;
    }

  }
}

int main(int argc, char *argv[]) {
  struct timeval beginTime, endTime;
  gettimeofday(&beginTime, NULL);
  char fileAddr[128] = "./Tuple";
  if (argc < 1) {
    printf("Too few Argument!\n");
    exit(-1);
  } else {
    DATA_SIZE = atol(argv[1]);
  }
  BUF_SIZE = 10ul;
  if (DATA_SIZE < 10ul) {
    BUF_SIZE = 1ul;
  }
  HOW_MANY_BUF = DATA_SIZE / BUF_SIZE;
  BUF_NUMBER = BUF_SIZE * (1ul << 24ul);
  strcat(fileAddr, argv[1]);
  FILE *dataFile = fopen(fileAddr, "rb");
  tuple *BUF = (tuple *) malloc(sizeof(tuple) * BUF_NUMBER);
  fread(BUF, sizeof(tuple), BUF_NUMBER, dataFile);
  printf("Before Sorting:%d\n", check(BUF));

  fastSort(BUF, 0, BUF_NUMBER - 1);
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000 * (endTime.tv_sec - beginTime.tv_sec) + endTime.tv_usec - beginTime.tv_usec;
  printf("Used Time:%lf s!\n", timeUsed / 1000000);
  printf("After Sorting:%d\n", check(BUF));
  fclose(dataFile);
  memset(BUF, 0, sizeof(tuple) * BUF_NUMBER);
  free(BUF);
  BUF = NULL;
  return 0;
}
//
// Created by DP on 2024/11/25.
//
