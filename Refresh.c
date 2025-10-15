#include<stdlib.h>
#include<string.h>
#include<stdio.h>
#include<mpi.h>
#include<time.h>
#include<sys/time.h>
#include<memory.h>
#include<math.h>
#include <unistd.h>

#define KEY_SIZE 8ul
#define REC_SIZE 56ul
#define P_MIN 2ul
#define WINDOW_SIZE 1ul << 10ul
#define GB 1ul << 24ul
#define EPSILON 1.0E-5f

typedef struct {
  uint64_t key;
  char record[REC_SIZE];
} tuple;

uint64_t DATA_SIZE = 0ul, TUPLE_TOTAL_COUNT = 0ul, TUPLE_SINGLE_COUNT = 0ul, ORDERED_TUPLES = 0ul;
uint8_t init_flag = 1;
uint64_t WINDOW_SIZE_CEILING = 0ul, WINDOW_SIZE_FLOOR = 0ul;
int32_t WORLD_RANK = 0, WORLD_SIZE = 0, WORKER_SIZE = 0;

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
uint64_t roundForU64(double a) {
  return (uint64_t) (a + 0.5f);
}
void Keys_Random(unsigned long p, unsigned long *single_keys) {
  for (unsigned long i = 0ul; i < p; i++) {
    unsigned long random_index = randomUnsignedLong();
    random_index = random_index % TUPLE_SINGLE_COUNT;
    single_keys[i] = TUPLES[random_index].key;
  }
}
uint64_t randomRange(uint64_t low, uint64_t high) {
  return low + randomUnsignedLong() % (high - low);
}
void Keys_RangeA(uint64_t p_old,uint64_t range_floor,uint64_t range_ceiling,uint64_t *single_keys) {
  for (uint64_t i=0;i<p_old-1;i++) {
    single_keys[i] = randomRange(range_floor, range_ceiling);
  }
  single_keys[p_old-1] = range_ceiling;
}
void Keys_RangeB(uint64_t p_old, uint64_t range_floor, uint64_t range_ceiling,uint64_t *single_keys) {
  for (uint64_t i=0;i<p_old;i++) {
    single_keys[i] = randomRange(range_floor, range_ceiling);
  }

}
void init(int argc, char **argv) {
  char *endptr = NULL;
  double tmp_window = (double) (WINDOW_SIZE);
  WINDOW_SIZE_CEILING = roundForU64(tmp_window * (1.0f + EPSILON));
  WINDOW_SIZE_FLOOR = roundForU64(tmp_window * (1.0f - EPSILON));
  DATA_SIZE = strtoul(argv[1], &endptr, 10);
  strcat(inputFileAddr, argv[1]);
  strcat(outputFileAddr, argv[1]);
  MPI_Comm_rank(MPI_COMM_WORLD, &WORLD_RANK);
  MPI_Comm_size(MPI_COMM_WORLD, &WORLD_SIZE);
  WORKER_SIZE = WORLD_SIZE - 1;
  TUPLE_SINGLE_COUNT = DATA_SIZE * GB;
  TUPLE_TOTAL_COUNT = DATA_SIZE * GB * WORKER_SIZE;
  TUPLES = (tuple *) malloc(TUPLE_SINGLE_COUNT * sizeof(tuple));
  memset(TUPLES, 0, TUPLE_SINGLE_COUNT * sizeof(tuple));
}
void master() {
  tuple *WINDOW_BUF=(tuple *) malloc(WINDOW_SIZE_CEILING* sizeof(tuple));

}
void worker() {
  FILE* inputFile = fopen(inputFileAddr, "r");
  fread(TUPLES, sizeof(tuple), TUPLE_SINGLE_COUNT, inputFile);
  fclose(inputFile);
  uint64_t MIN_KEY=UINT64_MAX, MAX_KEY = 0;
  uint64_t *SINGLE_KEYS=(uint64_t *) malloc((P_MIN+1)* sizeof(uint64_t)+2);
  for (uint64_t i=0;i<TUPLE_SINGLE_COUNT;i++) {
    if (TUPLES[i].key < MIN_KEY)
      MIN_KEY = TUPLES[i].key;
    if (TUPLES[i].key > MAX_KEY)
      MAX_KEY = TUPLES[i].key;
  }
  uint64_t p=P_MIN;
  uint64_t *single_keys=NULL,*total_keys=NULL,*single_counts=NULL,*total_counts=NULL;
  uint64_t near_batch=0;
  MPI_Status keys_status,conuts_status;
  while (ORDERED_TUPLES!=TUPLE_SINGLE_COUNT) {
    single_keys = (uint64_t *) malloc(p * sizeof(uint64_t));
    total_keys = (uint64_t *) malloc(p * WORKER_SIZE * sizeof(uint64_t));
    single_counts=(uint64_t *) malloc((p+1) * sizeof(uint64_t));
    total_counts=(uint64_t *) malloc((p+1) * WORKER_SIZE * sizeof(uint64_t));
    memset(single_keys, 0, sizeof(uint64_t)*p);
    memset(total_keys, 0, sizeof(uint64_t)*p*WORKER_SIZE);
    memset(single_counts, 0, sizeof(uint64_t)*(p+1));
    memset(total_counts, 0, sizeof(uint64_t)*(p+1)*WORKER_SIZE);
    if (init_flag==1) {
      init_flag = 0;
      Keys_Random(p, single_keys);
    }else {
      Keys_RangeA(p/2,MIN_KEY,near_batch,single_keys);
      Keys_RangeB(p/2,near_batch,MAX_KEY,single_keys+(p/2));
    }
    for (int i=0;i<WORKER_SIZE;) {
      if (i!=WORLD_RANK-1) {
        MPI_Send(single_keys,p,MPI_UINT64_T,i+1,44,MPI_COMM_WORLD);
      }
    }
    for (int i=0;i<WORKER_SIZE;i++) {
      if (i!=WORLD_RANK-1) {
        MPI_Recv(total_keys+i*p,p,MPI_UINT64_T,i+1,44,MPI_COMM_WORLD,&keys_status);
      }else {
      }
    }
  }
  sleep(10);
}
int main(int argc, char **argv) {
  init(argc, argv);
  if (WORLD_RANK == 0) {
    master();
  }else {

  }
}