#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <memory.h>
#include <mpi.h>
#include <limits.h>

#define KEY_SIZE 8ul
#define REC_SIZE 56ul
#define P_MIN 2ul
#define P_MAX 1ul>>10ul
#define S 1.15f

typedef struct {
  unsigned long key;
  char record[REC_SIZE];
} tuple;

unsigned long DATA_SIZE = 0ul, TUPLE_TOTAL_COUNT = 0ul, TUPLE_SINGLE_COUNT = 0ul, GB = 1ul << 24ul, WINDOW_SIZE = 0ul;
char *check_tuples = NULL;
char inputFileAddr[128] = "./Tuple", outputFileAddr[128] = "./SortedTuple";
tuple *TUPLES = NULL;
int WORLD_SIZE = 0, WORLD_RANK = 0, WORKER_SIZE = 0;

char checkSingleTuples() {
  for (unsigned long i = 0ul; i < TUPLE_SINGLE_COUNT; i++) {
    if (check_tuples[i] == 0) {
      return 1;
    }
  }
  return 0;
}
unsigned long randomUnsignedLong() {
  struct timespec time1={0,0};
  clock_gettime(CLOCK_REALTIME,&time1);
  srand(time1.tv_nsec);
  //srand((unsigned) time(NULL));
  int res[2];
  memset(&res, 0, sizeof(int) * 2);
  res[0] = rand() << 1;
  res[1] = rand();
  return *(unsigned long *) (&res);
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

void init(int argc, char **argv) {
  char *endptr = NULL;
  DATA_SIZE = strtoul(argv[1], &endptr, 10);
  WINDOW_SIZE = strtoul(argv[2], &endptr, 10);
  //WINDOW_SIZE = (unsigned long) ((double) S * WINDOW_SIZE);
  strcat(inputFileAddr, argv[1]);
  strcat(outputFileAddr, argv[1]);
  MPI_Comm_rank(MPI_COMM_WORLD, &WORLD_RANK);
  MPI_Comm_size(MPI_COMM_WORLD, &WORLD_SIZE);
  WORKER_SIZE = WORLD_SIZE - 1;
  TUPLE_SINGLE_COUNT = DATA_SIZE * GB;
  TUPLE_TOTAL_COUNT = DATA_SIZE * GB * (WORLD_SIZE - 1ul);
  TUPLES = (tuple *) malloc(TUPLE_SINGLE_COUNT * sizeof(tuple));
  check_tuples = (char *) malloc(TUPLE_SINGLE_COUNT * sizeof(char));
  memset(check_tuples, 0, sizeof(char) * TUPLE_SINGLE_COUNT);
}

void Keys_Random(unsigned long p, unsigned long *single_keys) {
  for (unsigned long i = 0ul; i < p; i++) {
    unsigned long random_index = randomUnsignedLong();
    random_index = random_index % TUPLE_SINGLE_COUNT;
    single_keys[i] = TUPLES[random_index].key;
  }
}

void Keys_Range(unsigned long p, unsigned long pivot_left, unsigned long pivot_right, unsigned long *single_keys) {
  unsigned long pivot_int = pivot_right - pivot_left + 1ul;
  for (unsigned long i = 0ul; i < p; i++) {
    unsigned long tmp=randomUnsignedLong()% pivot_int;
    single_keys[i] = tmp + pivot_left;
  }
}

void tap() {
  unsigned long recv_counts[WORLD_SIZE - 1ul];
  tuple *recv_tuples = NULL;
  int finish[WORLD_SIZE - 1ul];
  memset(&finish, 0, sizeof(int) * (WORLD_SIZE - 1ul));
  MPI_Status status;
  FILE *outputFile = fopen(outputFileAddr, "wb");
  while (1) {
    //unsigned batch_recv_count = 0ul;
    for (unsigned long i = 1ul; i < WORLD_SIZE; i++) {
      if (finish[i - 1] == 0) {
        int start_flag = 0;
        while (start_flag == 0) {
          MPI_Iprobe(i, 16, MPI_COMM_WORLD, &start_flag, &status);
        }
        MPI_Recv(&recv_counts[i - 1], 1, MPI_UNSIGNED_LONG, i, 16, MPI_COMM_WORLD, &status);
        printf("Tap receives worker %lu's count %lu", i, recv_counts[i - 1]);
      }
    }
    unsigned long j = 0ul;
    recv_tuples = (tuple *) malloc(sizeof(tuple) * WINDOW_SIZE);
    for (unsigned long i = 1ul; i < WORLD_SIZE; i++) {
      if (finish[i - 1] == 0) {
        if (recv_counts[i - 1] > 0) {
          MPI_Recv(recv_tuples + j, recv_counts[i - 1] * sizeof(tuple), MPI_CHAR, i, 44, MPI_COMM_WORLD, &status);
          j += recv_counts[i - 1];
        }
      }
    }
    int finish_all = 0;
    for (unsigned long i = 1ul; i < WORLD_SIZE; i++) {
      MPI_Iprobe(i, 5, MPI_COMM_WORLD, &finish[i - 1], &status);
      finish_all += finish[i - 1];
    }
    fastSort(recv_tuples, 0ul, WINDOW_SIZE - 1ul);
    fwrite(recv_tuples, sizeof(tuple), WINDOW_SIZE, outputFile);
    if (finish_all == WORLD_SIZE - 1) {
      free(recv_tuples);
      recv_tuples = NULL;
      fclose(outputFile);
      break;
    }
  }

}
/*void Sort() {
  unsigned long p = P_MIN;
  FILE *file = fopen(inputFileAddr, "rb");
  if (file == NULL) {
    perror("Unable to open file for reading");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  fread(TUPLES, sizeof(tuple), TUPLE_SINGLE_COUNT, file);
  fclose(file);

  int batch_found = 0, total_found = 0;
  unsigned long *all_keys = NULL, *all_counts = NULL, *final_counts = NULL;
  unsigned long total_key_count = 0ul, single_int_count = 0ul, total_int_count = 0ul,
      total_window_count = TUPLE_TOTAL_COUNT / WINDOW_SIZE;
  if (TUPLE_TOTAL_COUNT % WINDOW_SIZE != 0ul) {
    total_window_count += 1ul;
  }
  //pivot_keys = (unsigned long *)malloc(sizeof(unsigned long)*total_window_count);
  //memset(pivot_keys,0ul,sizeof(unsigned long)*total_window_count);
  unsigned long pivot_left = 0ul, pivot_right = 0ul, pivot = 0ul;
  MPI_Status status;
  while (1) {
    total_key_count = p * (WORLD_SIZE - 1ul);
    single_int_count = total_key_count + 1ul;
    total_int_count = single_int_count * (WORLD_SIZE - 1ul);
    // single_keys = (unsigned long *) malloc(p * sizeof(unsigned long));
    all_keys = (unsigned long *) malloc(total_key_count * sizeof(unsigned long));
    //memset(single_keys, 0ul, p * sizeof(unsigned long));
    memset(all_keys, 0ul, total_key_count * sizeof(unsigned long));
    if (WORLD_RANK == 1) {
      printf("p: %lu\n", p);
    }
    if (p == P_MIN) {
      Keys_Random(p, all_keys + p * (WORLD_RANK - 1ul));
    } else {
      if (batch_found == 1) {
        unsigned long pre_sic = (total_key_count << 1ul) + 1ul, sum = 0ul;
        for (unsigned long i = pivot; i < pre_sic; i++) {
          if (sum + final_counts[i] > WINDOW_SIZE) {
            Keys_Range(p, pivot_right, all_keys[i - 1], all_keys + p * (WORLD_RANK - 1ul));
          } else {
            sum += final_counts[i];
          }
        }
        batch_found = 0;
      } else {
        Keys_Range(p >> 1ul, pivot_left, pivot_right, all_keys + p * (WORLD_RANK - 1ul));
        Keys_Random(p >> 1ul, all_keys + p * (WORLD_RANK - 1ul) + (p >> 1ul));
      }
    }
    for (unsigned long i = 1; i < WORLD_SIZE; i++) {
      if (i != WORLD_RANK) {
        MPI_Send(all_keys + p * (WORLD_RANK - 1ul), p, MPI_UNSIGNED_LONG, i, 24, MPI_COMM_WORLD);
      }
    }
    for (unsigned long i = 1; i < WORLD_SIZE; i++) {
      if (i != WORLD_RANK) {
        MPI_Recv(all_keys + p * (i - 1ul), p, MPI_UNSIGNED_LONG, i, 24, MPI_COMM_WORLD, &status);
      }
    }
    //MPI_Allgather(single_keys, p, MPI_UNSIGNED_LONG, all_keys, total_key_count, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
    qsort(all_keys, total_key_count, sizeof(unsigned long), compare_keys);
    //single_counts = (unsigned long *) malloc(sizeof(unsigned long)*single_int_count);
    //memset(single_counts,0ul,sizeof(unsigned long)*single_int_count);
    all_counts = (unsigned long *) malloc(sizeof(unsigned long) * total_int_count);
    memset(all_counts, 0ul, sizeof(unsigned long) * total_int_count);
    for (unsigned long i = 0ul; i < TUPLE_SINGLE_COUNT; i++) {
      unsigned long range_index = find_range(all_keys, total_key_count, TUPLES[i].key);
      all_counts[single_int_count * (WORLD_RANK - 1) + range_index] += 1ul;
    }
    for (unsigned long i = 1ul; i < WORLD_SIZE; i++) {
      if (i != WORLD_RANK) {
        MPI_Send(all_counts + (single_int_count * (WORLD_RANK - 1ul)),
                 single_int_count,
                 MPI_UNSIGNED_LONG,
                 i,
                 24,
                 MPI_COMM_WORLD);
      }
    }
    for (unsigned long i = 1ul; i < WORLD_SIZE; i++) {
      if (i != WORLD_RANK) {
        MPI_Recv(all_counts + (single_int_count * (i - 1ul)),
                 single_int_count,
                 MPI_UNSIGNED_LONG,
                 i,
                 24,
                 MPI_COMM_WORLD,
                 &status);
        // printf("Worker %d,Recv i:%lu\n",WORLD_RANK,i);
      }
    }
    //MPI_Allgather(single_counts,single_int_count,MPI_UNSIGNED_LONG,all_counts,total_int_count,MPI_UNSIGNED_LONG,MPI_COMM_WORLD);
    final_counts = (unsigned long *) malloc(sizeof(unsigned long) * single_int_count);
    memset(final_counts, 0ul, sizeof(unsigned long) * single_int_count);
    for (unsigned long i = 0ul; i < single_int_count; i++) {
      for (unsigned long j = 1ul; j < WORLD_SIZE; j++) {
        final_counts[i] += all_counts[single_int_count * (j - 1) + i];
      }
    }
    if (WORLD_RANK == 2) {
      for (unsigned long i = 0ul; i < total_key_count; i++) {
        printf("i=%lu,all_keys[i]=%lx\n", i, all_keys[i]);
      }
      for (unsigned long i = 0ul; i < single_int_count; i++) {
        for (unsigned long j = 1ul; j < WORLD_SIZE; j++) {
          printf("%lu\t", all_counts[(j - 1) * single_int_count + i]);
        }
        printf("i=%lu,final_counts[i]=%lu\n", i, final_counts[i]);
      }
    }
    printf("Tuples_total_count:%lu\n", TUPLE_TOTAL_COUNT);
    unsigned long sum = 0ul;
    //printf("Worker:%d,SIC:%lu!\n",WORLD_RANK,single_int_count);
    //printf("Window Size:%lu\n",WINDOW_SIZE);
    for (unsigned long i = 0ul; i < single_int_count; i++) {
      if (final_counts[i] > WINDOW_SIZE) {
        printf("WORLD_RANK=%d,1\n", WORLD_RANK);
        break;
      } else if ((sum + final_counts[i]) > WINDOW_SIZE) {
        printf("WORLD_RANK=%d,2\n", WORLD_RANK);
        sum = 0ul;
        pivot_left = pivot_right;
        pivot_right = all_keys[i - 1];
        pivot = i;
        batch_found = 1ul;
        break;
      } else {
        printf("WORLD_RANK=%d,3\n", WORLD_RANK);
        sum += final_counts[i];
      }
    }
    printf("WORLD_RANK=%d,batch_found=%d", WORLD_RANK, batch_found);
    if (batch_found == 1) {
      tuple *send_batch = NULL;
      unsigned long batch_count = 0ul;
      for (unsigned long i = 0ul; i < TUPLE_SINGLE_COUNT; i++) {
        unsigned long key = TUPLES[i].key;
        if (key >= pivot_left && key < pivot_right) {
          check_tuples[i] = 1;
          batch_count += 1ul;
        }
      }
      MPI_Send(&batch_count, 1, MPI_UNSIGNED_LONG, 0, 16, MPI_COMM_WORLD);
      send_batch = (tuple *) malloc(sizeof(tuple) * batch_count);
      unsigned long j = 0ul;
      for (unsigned long i = 0ul; i < TUPLE_SINGLE_COUNT; i++) {
        unsigned long key = TUPLES[i].key;
        if (key >= pivot_right && key < pivot_right) {
          memcpy(send_batch + j, TUPLES + i, sizeof(tuple));
          j += 1ul;
        }
      }
      MPI_Send(send_batch, sizeof(tuple) * batch_count, MPI_CHAR, 0, 44, MPI_COMM_WORLD);
      free(send_batch);
      send_batch = NULL;
    }
    if (checkSingleTuples() == 0 || p == P_MAX) {
      printf("Worker %d check=%d", WORLD_RANK, checkSingleTuples());
      total_found = 1;
      unsigned long SIG_STOP = 5ul;
      MPI_Send(&SIG_STOP, 1, MPI_UNSIGNED_LONG, 0, 5, MPI_COMM_WORLD);
      break;
    }
    if (batch_found == 0) {

      p = p << 1ul;
    }
  }
  printf("Worker%d Bye!\n", WORLD_RANK);
}*/

void Sort2() {
  FILE *inputFile = fopen(inputFileAddr, "rb");
  fread(TUPLES, sizeof(tuple), TUPLE_SINGLE_COUNT, inputFile);
  fclose(inputFile);
  int total_found = 0, batch_found = 0;
  unsigned long p = P_MIN, total_keys_count = 0ul, single_int_count = 0ul, total_int_count = 0ul;
  unsigned long *KEYS = NULL, *COUNTS = NULL, *FINAL_COUNTS = NULL;
  unsigned long pivot_left = 0ul, pivot_right = 0ul, pivot = 0ul;
  unsigned long single_min_key = UINT64_MAX, single_max_key = 0ul;
  /*unsigned long *min_keys=(unsigned long *)malloc(sizeof(unsigned long)*WORKER_SIZE),
                *max_keys=(unsigned long *)malloc(sizeof(unsigned long)*WORKER_SIZE);
  memset(min_keys,UINT64_MAX,sizeof(unsigned long)*WORKER_SIZE);
  memset(max_keys,0,sizeof(unsigned long)*WORKER_SIZE);*/
  MPI_Status status;

  //Find min and max key
  for (unsigned long i = 0ul; i < TUPLE_SINGLE_COUNT; i++) {
    unsigned long key = TUPLES[i].key;
    if (key > single_max_key)
      single_max_key = key;
    if (key < single_min_key)
      single_min_key = key;
  }
  for (int i = 1; i <= WORKER_SIZE; i++) {
    if (i != WORLD_RANK) {
      MPI_Send(&single_min_key, 1, MPI_UNSIGNED_LONG, i, 7, MPI_COMM_WORLD);
      MPI_Send(&single_max_key, 1, MPI_UNSIGNED_LONG, i, 14, MPI_COMM_WORLD);
    }
  }
  unsigned long recv_min_key = 0ul, recv_max_key = 0ul;
  for (int i = 1; i <= WORKER_SIZE; i++) {
    if (i != WORLD_RANK) {
      MPI_Recv(&recv_min_key, 1, MPI_UNSIGNED_LONG, i, 7, MPI_COMM_WORLD, &status);
      MPI_Recv(&recv_max_key, 1, MPI_UNSIGNED_LONG, i, 14, MPI_COMM_WORLD, &status);
      if (recv_min_key < single_min_key)
        single_min_key = recv_min_key;
      if (recv_max_key > single_max_key)
        single_max_key = recv_max_key;
    }
  }
  pivot_left = single_min_key, pivot_right = single_min_key;
  if (WORLD_RANK == 1) {
    printf("Min key is 0x%lx, max key is 0x%lx\n", single_min_key, single_max_key);
    printf("WINDOW_SIZE is %lu\n", WINDOW_SIZE);
  }

  while (1) {
    total_keys_count = WORKER_SIZE * p;
    single_int_count = total_keys_count + 1ul;
    total_int_count = single_int_count * WORKER_SIZE;
    KEYS = (unsigned long *) malloc(sizeof(unsigned long) * total_keys_count);
    memset(KEYS, 0, sizeof(unsigned long) * total_keys_count);
    COUNTS = (unsigned long *) malloc(sizeof(unsigned long) * total_int_count);
    memset(COUNTS, 0, sizeof(unsigned long) * total_int_count);
    unsigned long *single_keys = KEYS + p * (WORLD_RANK - 1ul),
        *single_counts = COUNTS + single_int_count * (WORLD_RANK - 1ul);
    if (p == P_MIN) {
      Keys_Random(p, single_keys);
    } else {
      Keys_Range(p, pivot_left, pivot_right, single_keys);
    }


    //Exchange KEYS
    for (unsigned long i = 1ul; i <= WORKER_SIZE; i++) {
      if (i != WORLD_RANK) {
        MPI_Send(single_keys, p, MPI_UNSIGNED_LONG, i, 24, MPI_COMM_WORLD);
      }
    }
    for (unsigned long i = 1ul; i <= WORKER_SIZE; i++) {
      if (i != WORLD_RANK) {
        MPI_Recv(KEYS + (i - 1) * p, p, MPI_UNSIGNED_LONG, i, 24, MPI_COMM_WORLD, &status);
      }
    }
    qsort(KEYS, total_keys_count, sizeof(unsigned long), compare_keys);


    //Count by Keys
    for (unsigned long i = 0ul; i < TUPLE_SINGLE_COUNT; i++) {
      if (check_tuples[i] == 0) {
        unsigned long index = find_range(KEYS, total_keys_count, TUPLES[i].key);
        *(single_counts + index) += 1ul;
      }
    }

    //Exchange COUNTS
    for (unsigned long i = 1ul; i <= WORKER_SIZE; i++) {
      if (i != WORLD_RANK)
        MPI_Send(single_counts, single_int_count, MPI_UNSIGNED_LONG, i, 24, MPI_COMM_WORLD);
    }
    for (unsigned long i = 1ul; i <= WORKER_SIZE; i++) {
      if (i != WORLD_RANK)
        MPI_Recv(COUNTS + single_int_count * (i - 1),
                 single_int_count,
                 MPI_UNSIGNED_LONG,
                 i,
                 24,
                 MPI_COMM_WORLD,
                 &status);
    }
    //Collect COUNTS
    FINAL_COUNTS = (unsigned long *) malloc(sizeof(unsigned long) * single_int_count);
    memset(FINAL_COUNTS, 0, sizeof(unsigned long) * single_int_count);
    for (unsigned long i = 0ul; i < single_int_count; i++) {
      for (unsigned long j = 0ul; j < WORKER_SIZE; j++) {
        FINAL_COUNTS[i] += COUNTS[single_int_count * j + i];
      }
    }
   // printf("Worker %d:p=%lu\n", WORLD_RANK, p);
    //Control

    if(WORLD_RANK==1){
      printf("p=%lu\tpivot=%lu\tpivot_left=0x%lx\tpivot_right=0x%lx",p,pivot,pivot_left,pivot_right);
      /*for(unsigned long i=0ul;i<total_keys_count;i++){
        printf("KEYS[%lu]=%lx\t",i,KEYS[i]);
      }*/
      printf("\n");
    }
    unsigned long sum = 0ul;
    for (unsigned long i = 0ul; i < single_int_count; i++) {
      sum += FINAL_COUNTS[i];
      //printf("Worker %d:sum=%lu\n", WORLD_RANK, sum);
      if (sum == WINDOW_SIZE) {
        pivot = i;
        batch_found = 1;
        break;
      } else if (sum > WINDOW_SIZE) {
        if (i == 0) {
          if (KEYS[i] == single_min_key) {
            batch_found = 1;
            pivot = i;
          }else {
            batch_found = 0;
            if (i == single_int_count - 1ul) {
              pivot_right = single_max_key;
            } else{
              pivot_right = KEYS[i];
            }
          }
        } else {
          if (KEYS[i] == KEYS[i - 1]) {
            pivot = i;
            batch_found = 1;
          } else {
            batch_found = 0;
            if (i == single_int_count - 1ul) {
              pivot_right = single_max_key;
            } else{
              pivot_right = KEYS[i];
            }
          }
        }
        break;
      }
    }
    if (batch_found == 1) {
      if (pivot == single_int_count - 1ul)
        pivot_right = single_max_key;
      else
        pivot_right = KEYS[pivot];
      unsigned long batch_count = 0ul;
      for (unsigned long j = 0ul; j < TUPLE_SINGLE_COUNT; j++) {
        if (TUPLES[j].key < pivot_right && TUPLES[j].key >= pivot_left) {
          batch_count += 1ul;
        }
      }
      printf("Send count to tap!\n");
      MPI_Send(&batch_count, 1, MPI_UNSIGNED_LONG, 0, 16, MPI_COMM_WORLD);
      tuple *send_batch = (tuple *) malloc(sizeof(tuple) * batch_count);
      unsigned long k = 0ul;
      memset(send_batch, 0, sizeof(tuple) * batch_count);
      for (unsigned long j = 0ul; j < TUPLE_SINGLE_COUNT; j++) {
        if (TUPLES[j].key < pivot_right && TUPLES[j].key >= pivot_left) {
          check_tuples[j] = 1;
          memcpy(send_batch + k, TUPLES + j, sizeof(tuple));
        }
      }
      MPI_Send(send_batch, batch_count * sizeof(tuple), MPI_CHAR, 0, 44, MPI_COMM_WORLD);
      free(send_batch);
      send_batch = NULL;
      pivot_left = pivot_right + 1ul;
      unsigned long check_sum = 0ul;
      for (unsigned long j = pivot + 1ul; j < single_int_count; j++) {
        check_sum += FINAL_COUNTS[j];
        if (check_sum > WINDOW_SIZE) {
          if (j != single_int_count - 1ul)
            pivot_right = KEYS[j];
          else
            pivot_right = single_max_key;
        }
      }
      batch_found = 0;
    }
    //printf("WORKER FLUSH\n");
    //printf("Worker %d: inner cycle ends!\n",WORLD_RANK);
    total_found = (int) checkSingleTuples();
    p = p << 1ul;
    if (total_found == 0) {
      printf("Worker %d Bye!", WORLD_RANK);
      char SIG_STOP_SEND = 'a';
      MPI_Send(&SIG_STOP_SEND, 1, MPI_CHAR, 0, 5, MPI_COMM_WORLD);
      break;
    }
    if(p == P_MAX){
      break;
    }
    //printf("Worker %d:after p=%lu\n", WORLD_RANK, p);
  }
}
int main(int argc, char **argv) {
  struct timeval beginTime, endTime;
  gettimeofday(&beginTime, NULL);
  MPI_Init(NULL, NULL);
  init(argc, argv);
  if (WORLD_RANK == 0) {
    tap();
  } else {
    Sort2();
  }
  MPI_Finalize();
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000.0f * (endTime.tv_sec - beginTime.tv_sec) + endTime.tv_usec - beginTime.tv_usec;
  printf("Worker %d totally uses time :%lf s!\n", WORLD_RANK, timeUsed / 1000000);
}
