#include<stdlib.h>
#include<string.h>
#include<stdio.h>
#include<mpi.h>
#include<time.h>
#include<sys/time.h>
#include<memory.h>
#include<math.h>

#define KEY_SIZE 8ul
#define REC_SIZE 56ul
#define P_MIN 2ul
#define WINDOW_SIZE 1ul << 10ul
#define GB 1ul << 24ul
#define EPSILON 1.0E-5f;



typedef struct {
  uint64_t key;
  char record[REC_SIZE];
} tuple;

double TUPLE_TOTAL_COUNT=0.0f,TUPLE_SINGLE_COUNT=0.0f,ORDERED_TUPLES=0.0f;
uint8_t init_flag=1;
uint64_t near_batch=0ul;




char *check_tuples = NULL;
char inputFileAddr[128] = "./Tuple", outputFileAddr[128] = "./SortedTuple";
tuple *TUPLES = NULL;

unsigned long randomUnsignedLong() {
  struct timespec time1 = {0, 0};
  clock_gettime(CLOCK_REALTIME, &time1);
  srand(time1.tv_nsec);
  unsigned long i = (unsigned long) rand();
  i = i << 32;
  unsigned long j = (unsigned long) rand();
  j = j | i;
  return j;
}

int compare_keys(const void *a, const void *b) {
  unsigned long key1 = *(unsigned long *) a;
  unsigned long key2 = *(unsigned long *) b;
  return (key1 > key2) - (key1 < key2);
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

void partition(tuple *BUF, unsigned long low, unsigned long high, unsigned long *piv) {
  unsigned long pivot = BUF[high].key;
  unsigned long i = low - 1;
  for (unsigned long j = low; j < high; j++) {
    if (BUF[j].key < pivot) {
      i++;
      swap2(BUF + i, BUF + j);
    }
  }
  swap2(BUF + (i + 1), BUF + high);
  *piv = i + 1;
}
void fastSort(tuple *BUF, unsigned long low, unsigned high) {
  unsigned long stack[1024];
  unsigned long top = 0ul, piv = 0ull;
  stack[top++] = low;
  stack[top++] = high;
  while (top > 0) {
    high = stack[--top];
    low = stack[--top];
    partition(BUF, low, high, &piv);
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