#include<stdlib.h>
#include<string.h>
#include<stdio.h>
#include<stdint.h>
#include<mpi.h>
#include<time.h>
#include<sys/time.h>
#include<memory.h>
#include<math.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#define KEY_SIZE 8ul
#define REC_SIZE 56ul
#define P_MIN 2ul
#define P_MAX 256ul
#define WINDOW_SIZE 1ul << 20ul
#define GB 1ul << 24ul
#define EPSILON 0.2
#define OUTPUT_SWITCH 1

typedef struct {
  uint64_t key;
  char record[REC_SIZE];
} tuple;

uint64_t DATA_SIZE = 0ul, TUPLE_TOTAL_COUNT = 0ul, TUPLE_SINGLE_COUNT = 0ul, ORDERED_TUPLES = 0ul;
uint8_t init_flag = 1;
uint64_t WINDOW_SIZE_CEILING = 0ul, WINDOW_SIZE_FLOOR = 0ul;
int32_t WORLD_RANK = 0, WORLD_SIZE = 0, WORKER_SIZE = 0;

char inputFileAddr[128] = "/mnt/data/Sort/gyp/Tuple", outputFileAddr[128] = "/mnt/data/Sort/gyp/SortedTuple/";
tuple *TUPLES = NULL;
uint8_t *TUPLES_AVAILABLE = NULL;

void delete_files_in_dir(const char *path) {
  DIR *dir = opendir(path);
  if (!dir) {
    perror("opendir");
    return;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    // 跳过 . 和 ..
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    size_t path_len = strlen(path) + strlen(entry->d_name) + 2;
    char *full_path = malloc(path_len);
    snprintf(full_path, path_len, "%s/%s", path, entry->d_name);

    struct stat st;
    if (stat(full_path, &st) == 0 && S_ISREG(st.st_mode)) {
      if (unlink(full_path) == 0) {
        printf("Deleted: %s\n", full_path);
      } else {
        perror("unlink");
      }
    }

    free(full_path);
  }

  closedir(dir);
}

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

void Keys_RangeA(uint64_t p_old, uint64_t range_floor, uint64_t range_ceiling, uint64_t *single_keys) {
  for (uint64_t i = 0; i < p_old - 1; i++) {
    single_keys[i] = randomRange(range_floor, range_ceiling);
  }
  single_keys[p_old - 1] = range_ceiling;
}

void Keys_RangeB(uint64_t p_old, uint64_t range_floor, uint64_t range_ceiling, uint64_t *single_keys) {
  for (uint64_t i = 0; i < p_old; i++) {
    single_keys[i] = randomRange(range_floor, range_ceiling);
  }
}
void tap(uint64_t range_floor, uint64_t range_ceiling, uint64_t tap_counts) {
  if (OUTPUT_SWITCH == 1 && WORLD_RANK == 1) {
    printf("Worker %d will tap %lu tuples to Master!\n", WORLD_RANK - 1, tap_counts);
    printf("Worker %d TRF is %lu,TRC is %lu!\n", WORLD_RANK - 1, range_floor, range_ceiling);
  }
  MPI_Request count_request, tuple_request;
  tuple *sendBuf = (tuple *) malloc(tap_counts * sizeof(tuple));
  uint64_t bufCount = 0ul;
  for (uint64_t i = 0; i < TUPLE_SINGLE_COUNT; i++) {
    if (TUPLES_AVAILABLE[i] == 1) {
      if (TUPLES[i].key > range_floor && TUPLES[i].key < range_ceiling || TUPLES[i].key == range_floor) {
        memcpy(sendBuf+bufCount, &TUPLES[i], sizeof(tuple));
        bufCount++;
        TUPLES_AVAILABLE[i]=0;
      }
    }
  }
  if (OUTPUT_SWITCH == 1 && WORLD_RANK == 1) {
    printf("Worker %d has found %lu tuples in this range actually!\n", WORLD_RANK - 1, bufCount);
  }
  MPI_Isend(&bufCount, 1,MPI_UINT64_T, 0, 77,MPI_COMM_WORLD, &count_request);
  //MPI_Send(&bufCount, 1,MPI_UINT64_T, 0, 77,MPI_COMM_WORLD);
  MPI_Wait(&count_request,MPI_STATUS_IGNORE);
  if (bufCount != 0) {
    MPI_Isend(sendBuf, bufCount * sizeof(tuple),MPI_CHAR, 0, 77,MPI_COMM_WORLD, &tuple_request);
  }
  MPI_Wait(&tuple_request,MPI_STATUS_IGNORE);
  free(sendBuf);
  sendBuf = NULL;
}
void init(int argc, char **argv) {
  char *endptr = NULL;
  double tmp_window = (double) (WINDOW_SIZE);
  WINDOW_SIZE_CEILING = roundForU64(tmp_window * (1.0f + EPSILON));
  WINDOW_SIZE_FLOOR = roundForU64(tmp_window * (1.0f - EPSILON));
  DATA_SIZE = strtoul(argv[1], &endptr, 10);
  strcat(inputFileAddr, argv[1]);
  MPI_Comm_rank(MPI_COMM_WORLD, &WORLD_RANK);
  MPI_Comm_size(MPI_COMM_WORLD, &WORLD_SIZE);
  WORKER_SIZE = WORLD_SIZE - 1;
  TUPLE_SINGLE_COUNT = DATA_SIZE * GB;
  TUPLE_TOTAL_COUNT = DATA_SIZE * GB * WORKER_SIZE;
  printf("WR is %d,WDS is %d,WSC is %lu,WSF is %lu\n", WORLD_RANK, WORLD_SIZE, WINDOW_SIZE_CEILING, WINDOW_SIZE_FLOOR);
}

unsigned long find_range(unsigned long *sorted_keys, unsigned long key_count, unsigned long key) {
  unsigned long left = 0ul;
  unsigned long right = key_count;
  while (left < right) {
    unsigned long mid = left + (right - left) / 2ul;
    if (key <= sorted_keys[mid]) {
      right = mid;
    } else {
      left = mid + 1ul;
    }
  }
  return left;
}

void master() {
  uint64_t recv_count = 0ul, batch = 0ul, sum_counts = 0ul;
  delete_files_in_dir(outputFileAddr);
  uint64_t tuple_counts[WORKER_SIZE];
  tuple *recvBuf = NULL;
  char batchOutputFileAddr[128], batchNumber[16];
  MPI_Request count_requests[WORKER_SIZE];
  int non_zero_worker_size = WORKER_SIZE;
  //printf("Master World Rank : %d \n", WORLD_RANK);
  while (recv_count != TUPLE_TOTAL_COUNT) {
    sum_counts = 0ul;
    non_zero_worker_size = WORKER_SIZE;
    memset(tuple_counts, 0, WORKER_SIZE*sizeof(uint64_t));
    for (int i = 1; i <= WORKER_SIZE; i++) {
      MPI_Irecv(&tuple_counts[i - 1], 1,MPI_UINT64_T, i, 77,MPI_COMM_WORLD, &count_requests[i - 1]);
    }
    MPI_Waitall(WORKER_SIZE, count_requests,MPI_STATUS_IGNORE);
    for (int i = 0; i < WORKER_SIZE; i++) {
      printf("Master will receive %lu tuples from Worker %d\n", tuple_counts[i], i + 1);
      sum_counts += tuple_counts[i];
      if (tuple_counts[i] == 0) {
        non_zero_worker_size -= 1;
      }
    }
    /*if (OUTPUT_SWITCH==1) {
      printf("Master says hi here1!\n");
    }*/
    MPI_Request tuple_requests[non_zero_worker_size];
    recvBuf = (tuple *) realloc(recvBuf, sum_counts * sizeof(tuple));
    memset(recvBuf, 0, sum_counts * sizeof(tuple));
    tuple *Bufit = recvBuf;
    int req_it = 0;
    /*if (OUTPUT_SWITCH==1) {
      printf("Master says hi here2!\n");
    }*/
    for (int i = 1; i <= WORKER_SIZE; i++) {
      if (tuple_counts[i - 1] != 0ul) {
        if (OUTPUT_SWITCH == 1) {
          printf("Master receives from %lu Worker %d\n", tuple_counts[i - 1], i - 1);
        }
        MPI_Irecv(Bufit, tuple_counts[i - 1] * sizeof(tuple),MPI_CHAR, i, 77,MPI_COMM_WORLD, &tuple_requests[req_it++]);
        Bufit += tuple_counts[i - 1];
      }
    }
    MPI_Waitall(non_zero_worker_size, tuple_requests,MPI_STATUS_IGNORE);
    printf("Master receives all tuple%lu!\n", batch);
    fastSort(recvBuf, 0, sum_counts - 1);
    memset(batchNumber, 0, 16*sizeof(char));
    memset(batchOutputFileAddr, 0, 128*sizeof(char));
    snprintf(batchNumber, 16, "%lu", batch);
    strcpy(batchOutputFileAddr, outputFileAddr);
    strcat(batchOutputFileAddr, batchNumber);
    batch += 1;
    FILE *outputFile = fopen(batchOutputFileAddr, "w");
    fwrite(recvBuf, sizeof(tuple), sum_counts, outputFile);
    fclose(outputFile);
    free(recvBuf);
    recvBuf = NULL;
    recv_count += sum_counts;
    sum_counts = 0ul;
  }
  printf("Hello from Master !8");
  free(recvBuf);
  recvBuf = NULL;
}

void worker() {
  FILE *inputFile = fopen(inputFileAddr, "r");
  fread(TUPLES, sizeof(tuple), TUPLE_SINGLE_COUNT, inputFile);
  fclose(inputFile);
  uint64_t MINS[WORKER_SIZE], MAXS[WORKER_SIZE];
  MINS[WORLD_RANK - 1] = UINT64_MAX, MAXS[WORLD_RANK - 1] = 0;
  uint64_t MIN_KEY = UINT64_MAX, MAX_KEY = 0ul, p = P_MIN, near_batch = 0ul, sum = 0ul;
  uint64_t *single_keys = NULL, *total_keys = NULL, *single_counts = NULL, *total_counts = NULL, *sum_counts = NULL;
  MPI_Request min_req[(WORKER_SIZE - 1) * 2], max_req[(WORKER_SIZE - 1) * 2], key_req[(WORKER_SIZE - 1) * 2], counts_req
      [(WORKER_SIZE - 1) * 2];
  int min_key_it = 0, max_key_it = 0, key_req_it = 0, counts_req_it = 0;
  TUPLES_AVAILABLE = (uint8_t *) malloc(TUPLE_SINGLE_COUNT * sizeof(uint8_t));
  memset(TUPLES_AVAILABLE, 1, TUPLE_SINGLE_COUNT*sizeof(uint8_t));
  for (uint64_t i = 0; i < TUPLE_SINGLE_COUNT; i++) {
    if (TUPLES[i].key < MINS[WORLD_RANK - 1])
      MINS[WORLD_RANK - 1] = TUPLES[i].key;
    if (TUPLES[i].key > MAXS[WORLD_RANK - 1])
      MAXS[WORLD_RANK - 1] = TUPLES[i].key;
  }
  //max_key_it=0,min_key_it=0;
  for (int i = 0; i < WORKER_SIZE; i++) {
    if (i != WORLD_RANK - 1) {
      MPI_Isend(&MINS[WORLD_RANK - 1], 1,MPI_UINT64_T, i + 1, 81,MPI_COMM_WORLD, &min_req[min_key_it++]);
      MPI_Isend(&MAXS[WORLD_RANK - 1], 1,MPI_UINT64_T, i + 1, 63,MPI_COMM_WORLD, &max_req[max_key_it++]);
    }
  }
  for (int i = 0; i < WORKER_SIZE; i++) {
    if (i != WORLD_RANK - 1) {
      MPI_Irecv(MINS + i, 1,MPI_UINT64_T, i + 1, 81,MPI_COMM_WORLD, &min_req[min_key_it++]);
      MPI_Irecv(MAXS + i, 1,MPI_UINT64_T, i + 1, 63,MPI_COMM_WORLD, &max_req[max_key_it++]);
    }
  }
  MPI_Waitall((WORKER_SIZE - 1) * 2, min_req,MPI_STATUS_IGNORE);
  MPI_Waitall((WORKER_SIZE - 1) * 2, max_req,MPI_STATUS_IGNORE);
  for (int i = 0; i < WORKER_SIZE; i++) {
    if (MIN_KEY > MINS[i]) {
      MIN_KEY = MINS[i];
    }
    if (MAX_KEY < MAXS[i]) {
      MAX_KEY = MAXS[i];
    }
  }
  if (OUTPUT_SWITCH == 1) {
    printf("WORKER %d found min %lx and max %lx!\n", WORLD_RANK - 1, MIN_KEY, MAX_KEY);
  }

  uint64_t total_keys_size = p * WORKER_SIZE;
  uint64_t single_counts_size = total_keys_size + 1ul;
  uint64_t total_counts_size = single_counts_size * WORKER_SIZE;
  uint64_t sum_counts_size = single_counts_size;
  //single_keys = (uint64_t *) malloc(p * sizeof(uint64_t));
  total_keys = (uint64_t *) malloc(total_keys_size * sizeof(uint64_t));
  single_counts = (uint64_t *) malloc(single_counts_size * sizeof(uint64_t));
  total_counts = (uint64_t *) malloc(total_counts_size * sizeof(uint64_t));
  sum_counts = (uint64_t *) malloc(sum_counts_size * sizeof(uint64_t));
  uint64_t single_keys_bias = (WORLD_RANK - 1) * p;
  while (ORDERED_TUPLES != TUPLE_TOTAL_COUNT) {
    if (OUTPUT_SWITCH == 1 && WORLD_RANK == 1) {
      printf("\n\nP is %lu\n", p);
    }
    total_keys_size = p * WORKER_SIZE;
    single_counts_size = total_keys_size + 1ul;
    total_counts_size = single_counts_size * WORKER_SIZE;
    sum_counts_size = single_counts_size;
    single_keys_bias = (WORLD_RANK - 1) * p;
    //printf("WORKER %d,Here6!\n",WORLD_RANK-1);
    if (init_flag == 1 || p == P_MAX) {
      if (p == P_MAX) {
        //single_keys = (uint64_t *) realloc(single_keys, p * sizeof(uint64_t));
        total_keys = (uint64_t *) realloc(total_keys, total_keys_size * sizeof(uint64_t));
        single_counts = (uint64_t *) realloc(single_counts, single_counts_size * sizeof(uint64_t));
        total_counts = (uint64_t *) realloc(total_counts, total_counts_size * sizeof(uint64_t));
        sum_counts = (uint64_t *) realloc(sum_counts, sum_counts_size * sizeof(uint64_t));
      }
      single_keys = total_keys + single_keys_bias;
      init_flag = 0;
      //memset(single_keys, 0, sizeof(uint64_t) * p);
      memset(total_keys, 0, sizeof(uint64_t) * total_keys_size);
      memset(single_counts, 0, sizeof(uint64_t) *single_counts_size);
      memset(total_counts, 0, sizeof(uint64_t) * total_counts_size);
      memset(sum_counts, 0, sizeof(uint64_t) * sum_counts_size);
      Keys_Random(p, single_keys);
    } else {
      total_keys = (uint64_t *) realloc(total_keys, total_keys_size * sizeof(uint64_t));
      single_counts = (uint64_t *) realloc(single_counts, single_counts_size * sizeof(uint64_t));
      total_counts = (uint64_t *) realloc(total_counts, total_counts_size * sizeof(uint64_t));
      sum_counts = (uint64_t *) realloc(sum_counts, sum_counts_size * sizeof(uint64_t));
      memset(total_keys, 0, sizeof(uint64_t) * total_keys_size);
      memset(single_counts, 0, sizeof(uint64_t) * single_counts_size);
      memset(total_counts, 0, sizeof(uint64_t) * total_counts_size);
      memset(sum_counts, 0, sizeof(uint64_t) * sum_counts_size);
      single_keys = total_keys + single_keys_bias;
      Keys_RangeB(p / 2, MIN_KEY, near_batch, single_keys);
      Keys_RangeB(p / 2, near_batch, MAX_KEY, single_keys + (p / 2));
    }
    /*if (OUTPUT_SWITCH==1&&WORLD_RANK==1) {
      printf("Single Keys:\n");
      for (uint64_t i=0ul;i<p;i++) {
        printf("0x%lx\t", total_keys[i]);
      }
      printf("\n");
    }*/
    key_req_it = 0;
    for (int i = 0; i < WORKER_SIZE; i++) {
      if (i != WORLD_RANK - 1) {
        MPI_Isend(single_keys, p,MPI_UINT64_T, i + 1, 44,MPI_COMM_WORLD, &key_req[key_req_it++]);
        single_keys = total_keys + single_keys_bias;
      }
    }
    for (int i = 0; i < WORKER_SIZE; i++) {
      if (i != WORLD_RANK - 1) {
        MPI_Irecv(total_keys + i * p, p,MPI_UINT64_T, i + 1, 44,MPI_COMM_WORLD, &key_req[key_req_it++]);
      }
    }
    MPI_Waitall((WORKER_SIZE - 1) * 2, key_req,MPI_STATUS_IGNORE);
    /*if (OUTPUT_SWITCH==1&&WORLD_RANK==1) {
      printf("Total Keys:\n");
      for (uint64_t i=0ul;i<total_keys_size;i++) {
        printf("0x%lx\t", total_keys[i]);
      }
      printf("\n");
    }*/
    qsort(total_keys, total_keys_size, sizeof(uint64_t), compare_keys);
    if (OUTPUT_SWITCH == 1 && WORLD_RANK == 1) {
      printf("Sorted Keys:\n");
      for (uint64_t i = 0ul; i < total_keys_size; i++) {
        printf("0x%lx\t", total_keys[i]);
      }
      printf("\n");
    }
    for (uint64_t i = 0ul; i < TUPLE_SINGLE_COUNT; i++) {
      if (TUPLES_AVAILABLE[i] == 1) {
        uint64_t index = find_range(total_keys, total_keys_size, TUPLES[i].key);
        single_counts[index] += 1ul;
      }
    }
    /*if (OUTPUT_SWITCH==1&&WORLD_RANK==1) {
      printf("Single Counts:\n");
      for (uint64_t i=0ul;i<single_counts_size;i++) {
        printf("%lu\t", single_counts[i]);
      }
      printf("\n");
    }*/
    counts_req_it = 0ul;
    for (int i = 0; i < WORKER_SIZE; i++) {
      if (i != WORLD_RANK - 1) {
        MPI_Isend(single_counts,
                  single_counts_size,
                  MPI_UINT64_T,
                  i + 1,
                  16,
                  MPI_COMM_WORLD,
                  &counts_req[counts_req_it++]);
      }
    }
    for (int i = 0; i < WORKER_SIZE; i++) {
      if (i != WORLD_RANK - 1) {
        MPI_Irecv(total_counts + single_counts_size * i,
                  single_counts_size,
                  MPI_UINT64_T,
                  i + 1,
                  16,
                  MPI_COMM_WORLD,
                  &counts_req[counts_req_it++]);
      } else {
        memcpy(total_counts + single_counts_size * i, single_counts, single_counts_size * sizeof(uint64_t));
      }
    }
    MPI_Waitall((WORKER_SIZE - 1) * 2, counts_req,MPI_STATUS_IGNORE);
    /*if (OUTPUT_SWITCH==1&&WORLD_RANK==1) {
      printf("Total Counts:\n");
      for (uint64_t i=0ul;i<total_counts_size;i++) {
        printf("%lu\t", total_counts[i]);
      }
      printf("\n");
    }*/
    for (uint64_t i = 0ul; i < sum_counts_size; i++) {
      for (int j = 0; j < WORKER_SIZE; j++) {
        sum_counts[i] += total_counts[j * single_counts_size + i];
      }
    }
    if (OUTPUT_SWITCH == 1 && WORLD_RANK == 1) {
      printf("Sum Counts:\n");
      for (uint64_t i = 0ul; i < sum_counts_size; i++) {
        printf("%lu\t", sum_counts[i]);
      }
      printf("\n");
    }
    sum = 0ul;
    fflush(stdout);
    for (uint64_t i = 0ul; i < sum_counts_size; i++) {
      sum = sum + sum_counts[i];
      if (OUTPUT_SWITCH == 1 && WORLD_RANK == 1) {
        printf("SUM is %lu!\n", sum);
      }
      if (sum > WINDOW_SIZE_CEILING) {
        uint64_t old_sum = sum - sum_counts[i];
        if (old_sum > WINDOW_SIZE_FLOOR) {
          tap(MIN_KEY, total_keys[i - 1], old_sum);
          MIN_KEY = total_keys[i - 1];
          init_flag = 1;
          sum = 0ul;
          break;
        } else {
          if (p != P_MAX) {
            p = p * 2;
          }
          near_batch = total_keys[i];
          if (OUTPUT_SWITCH == 1 && WORLD_RANK == 1) {
            printf("Near_batch is 0x%lx! \n", near_batch);
          }
          sum = 0ul;
          break;
        }
      }
    }
    //printf("WORKER %d,%luHere8! \n", WORLD_RANK - 1, p);
  }
  //Free Memory
  free(single_keys);
  free(total_keys);
  free(single_counts);
  free(total_counts);
  free(sum_counts);
  single_keys = NULL;
  total_keys = NULL;
  single_counts = NULL;
  total_counts = NULL;
  sum_counts = NULL;
}

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  init(argc, argv);
  if (WORLD_RANK == 0) {
    master();
  } else {
    TUPLES = (tuple *) malloc(TUPLE_SINGLE_COUNT * sizeof(tuple));
    memset(TUPLES, 0, TUPLE_SINGLE_COUNT * sizeof(tuple));
    worker();
  }
  free(TUPLES);
  TUPLES = NULL;
  MPI_Finalize();
  return 0;
}
