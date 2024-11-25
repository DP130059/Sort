#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>
#include <sys/mman.h>
#include <memory.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>

#define KEY_SIZE 8ul
#define REC_SIZE 56ul
#define THREAD_NUMBER 17

typedef struct {
  unsigned long key;
  unsigned char record[REC_SIZE];
} tuple;

typedef struct {
  unsigned long key;
  unsigned long index;
} keyPair;

unsigned long DATA_SIZE = 0ul, BUF_SIZE = 0ul;
char inputFileaddr[128] = "./Tuple", outputFileaddr[128] = "./SortedTuple";
char SIGSTOPSEND = 'a';

void swap(tuple *t1, tuple *t2) {
  tuple *tmp = (tuple *) malloc(sizeof(tuple));
  memset(tmp, 0, sizeof(tuple));
  memcpy(tmp, t1, sizeof(tuple));
  memcpy(t1, t2, sizeof(tuple));
  memcpy(t2, tmp, sizeof(tuple));
  free(tmp);
  tmp = NULL;
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
  memcpy(&tmp.key, &t1->key, sizeof(unsigned long));
  memcpy(&t1->key, &t2->key, sizeof(unsigned long));
  memcpy(&t2->key, &tmp.key, sizeof(unsigned long));
  memcpy(&tmp.record, t1->record, sizeof(char) * REC_SIZE);
  memcpy(t1->record, t2->record, sizeof(char) * REC_SIZE);
  memcpy(t2->record, &tmp.record, sizeof(char) * REC_SIZE);
}
void keySwap(keyPair *t1, keyPair *t2) {
  keyPair tmp;
  tmp.key = t1->key;
  t1->key = t2->key;
  t2->key = tmp.key;
  tmp.index = t1->index;
  t1->index = t2->index;
  t2->index = tmp.index;
}

void keyPartition(keyPair *BUF, unsigned long low, unsigned long high, unsigned long *piv) {
  unsigned long pivot = BUF[high].key;
  unsigned long i = low - 1;
  for (unsigned long j = low; j < high; j++) {
    if (BUF[j].key < pivot) {
      i++;
      keySwap(BUF + i, BUF + j);
    }
  }
  keySwap(BUF + (i + 1), BUF + high);
  *piv = i + 1;
}
void keyFastSort(keyPair *BUF, unsigned long low, unsigned high) {
  unsigned long stack[1024];
  unsigned long top = 0ul, piv = 0ull;
  stack[top++] = low;
  stack[top++] = high;
  while (top > 0) {
    high = stack[--top];
    low = stack[--top];
    keyPartition(BUF, low, high, &piv);
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
char check(tuple *BUF, unsigned long BUF_LENGTH) {
  for (unsigned i = 0; i < BUF_LENGTH - 1; i++) {
    if (BUF[i].key > BUF[i + 1].key) {
      return -1;
    }
  }
  return 1;
}

int getBucket(unsigned long key) {
  unsigned long longKey = ((key >> 60) & 0xF);
  int shortKey = (int) longKey;
  return shortKey + 1;
}

void init(int argc, char *argv[]) {
  if (argc < 1) {
    printf("Too Few Arguments!\n");
    exit(-1);
  }
  DATA_SIZE = atol(argv[1]);
  strcat(inputFileaddr, argv[1]);
  strcat(outputFileaddr, argv[1]);
  BUF_SIZE = DATA_SIZE << 24ul;
}

void master() {
  struct timeval startTime, endTime;
  gettimeofday(&startTime, NULL);
  int fd = open(inputFileaddr, O_RDWR), recvStartflag = 0, recvEndflag = 0;
  unsigned long totalCount = 0ul;
  tuple *BUF = NULL, *recvBUF = NULL;
  MPI_Status status;
  BUF = (tuple *) mmap(NULL, BUF_SIZE * sizeof(tuple), PROT_READ, MAP_SHARED, fd, 0);
  madvise(BUF, BUF_SIZE * sizeof(tuple), 2);
  printf("Master start sending!\n");
  for (int j = 0; j < BUF_SIZE; j++) {
    int dest = getBucket(BUF[j].key);
    MPI_Send(BUF + j, sizeof(tuple), MPI_CHAR, dest, 0, MPI_COMM_WORLD);
  }
  printf("Master finish sending!\n");
  for (int i = 1; i < THREAD_NUMBER; i++) {
    MPI_Send(&SIGSTOPSEND, 1, MPI_CHAR, i, 0, MPI_COMM_WORLD);
    printf("Sending stop signal to worker,%d!\n", i);
  }
  munmap(BUF, sizeof(tuple) * BUF_SIZE);
  close(fd);
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Master sending uses time :%lf s!\n", timeUsed / 1000000);
  FILE *outputFile = fopen(outputFileaddr, "wb");
  recvBUF = (tuple *) malloc(sizeof(tuple) * BUF_SIZE);
  memset(recvBUF, 0, sizeof(tuple) * BUF_SIZE);
  for (int i = 1; i < THREAD_NUMBER; i++) {
    unsigned long divideCount = 0;
    printf("Master starts collecting data from worker %d\n", i);
    while (recvStartflag == 0) {
      MPI_Iprobe(i, 1, MPI_COMM_WORLD, &recvStartflag, &status);
    }
    MPI_Recv(&divideCount, 1, MPI_UNSIGNED_LONG, i, 1, MPI_COMM_WORLD, &status);
    if (divideCount != 0) {
      MPI_Recv(recvBUF + totalCount, sizeof(tuple) * divideCount, MPI_CHAR, i, 1, MPI_COMM_WORLD, &status);
      totalCount += divideCount;
    }
  }
  fwrite(recvBUF, sizeof(tuple), BUF_SIZE, outputFile);
  fclose(outputFile);
  gettimeofday(&endTime, NULL);
  timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Master totally uses time :%lf s!\n", timeUsed / 1000000);
  free(recvBUF);
  recvBUF = NULL;
}

void master2() {
  struct timeval startTime, endTime;
  gettimeofday(&startTime, NULL);
  int fd = open(inputFileaddr, O_RDWR), recvStartflag = 0, recvEndflag = 0, packSize = 0;
  unsigned long totalCount = 0ul;
  tuple *BUF = NULL, *recvBUF = NULL;
  MPI_Status status;
  BUF = (tuple *) mmap(NULL, BUF_SIZE * sizeof(tuple), PROT_READ, MAP_SHARED, fd, 0);
  madvise(BUF, BUF_SIZE * sizeof(tuple), 2);
  MPI_Pack_size(sizeof(tuple), MPI_CHAR, MPI_COMM_WORLD, &packSize);
  void *sendBuf = malloc(sizeof(tuple) * sizeof(char) + MPI_BSEND_OVERHEAD);
  MPI_Buffer_attach(sendBuf, packSize + MPI_BSEND_OVERHEAD);
  printf("Master start sending!\n");
  for (int j = 0; j < BUF_SIZE; j++) {
    int dest = getBucket(BUF[j].key);
    MPI_Bsend(BUF + j, sizeof(tuple), MPI_CHAR, dest, 0, MPI_COMM_WORLD);
  }
  MPI_Buffer_detach(&sendBuf, &packSize);
  printf("Master finish sending!\n");
  memset(sendBuf, 0, sizeof(tuple) * sizeof(char) + MPI_BSEND_OVERHEAD);
  free(sendBuf);
  sendBuf = NULL;
  for (int i = 1; i < THREAD_NUMBER; i++) {
    MPI_Send(&SIGSTOPSEND, 1, MPI_CHAR, i, 0, MPI_COMM_WORLD);
  }
  munmap(BUF, sizeof(tuple) * BUF_SIZE);
  close(fd);
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Master sending uses time :%lf s!\n", timeUsed / 1000000);
  FILE *outputFile = fopen(outputFileaddr, "wb");
  recvBUF = (tuple *) malloc(sizeof(tuple) * BUF_SIZE);
  memset(recvBUF, 0, sizeof(tuple) * BUF_SIZE);
  for (int i = 1; i < THREAD_NUMBER; i++) {
    unsigned long divideCount = 0;
    while (recvStartflag == 0) {
      MPI_Iprobe(i, 1, MPI_COMM_WORLD, &recvStartflag, &status);
    }
    MPI_Recv(&divideCount, 1, MPI_UNSIGNED_LONG, i, 1, MPI_COMM_WORLD, &status);
    if (divideCount != 0) {
      MPI_Recv(recvBUF + totalCount, sizeof(tuple) * divideCount, MPI_CHAR, i, 1, MPI_COMM_WORLD, &status);
      totalCount += divideCount;
    }
  }
  fwrite(recvBUF, sizeof(tuple), BUF_SIZE, outputFile);
  fclose(outputFile);
  gettimeofday(&endTime, NULL);
  timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Master totally uses time :%lf s!\n", timeUsed / 1000000);
  free(recvBUF);
  recvBUF = NULL;
}

void splitmaster() {
  struct timeval startTime, endTime;
  gettimeofday(&startTime, NULL);
  int fd = open(inputFileaddr, O_RDWR), recvStartflag = 0, recvEndflag = 0;
  unsigned long totalCount = 0ul;
  tuple *BUF = NULL, *writeBUF = NULL;
  MPI_Status status;
  BUF = (tuple *) mmap(NULL, BUF_SIZE * sizeof(tuple), PROT_READ, MAP_SHARED, fd, 0);
  madvise(BUF, BUF_SIZE * sizeof(tuple), 2);
  keyPair *sendPairs[THREAD_NUMBER];
  for (int i = 0; i < THREAD_NUMBER; i++) {
    sendPairs[i] = (keyPair *) malloc(sizeof(keyPair) * BUF_SIZE);
    memset(sendPairs[i], 0, sizeof(keyPair) * BUF_SIZE);
  }
  unsigned long sendCounts[THREAD_NUMBER];
  memset(sendCounts, 0, sizeof(unsigned long) * THREAD_NUMBER);
  for (unsigned long j = 0ul; j < BUF_SIZE; j++) {
    int dest = getBucket(BUF[j].key);
    sendPairs[dest - 1][sendCounts[dest - 1]].key = BUF[j].key;
    sendPairs[dest - 1][sendCounts[dest - 1]].index = j;
    sendCounts[dest - 1]++;
  }
  printf("Master start sending!\n");
  for (int i = 1; i < THREAD_NUMBER; i++) {
    MPI_Send(&sendCounts[i-1], 1, MPI_UNSIGNED_LONG, i, 0, MPI_COMM_WORLD);
    MPI_Send(sendPairs[i - 1], sizeof(keyPair) * sendCounts[i - 1], MPI_CHAR, i, 0, MPI_COMM_WORLD);
  }
  printf("Master finish sending!\n");
  madvise(BUF,sizeof(tuple)*BUF_SIZE,1);
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Master sending uses time :%lf s!\n", timeUsed / 1000000);
  FILE *outputFile = fopen(outputFileaddr, "wb");
  for (int i = 1; i < THREAD_NUMBER; i++) {
    printf("Master starts collecting data from worker %d\n", i);
    MPI_Recv(sendPairs[i-1],sizeof(keyPair)*sendCounts[i-1],MPI_CHAR,i,0,MPI_COMM_WORLD,&status);
  }
  writeBUF = (tuple *) malloc(sizeof(tuple) * BUF_SIZE);
  memset(writeBUF, 0, sizeof(tuple) * BUF_SIZE);
  unsigned long writeCount=0ul,writeIndex=0ul;
  for(int i=1;i<THREAD_NUMBER;i++){
    for(unsigned long j=0ul;j<sendCounts[i-1];j++){
      writeBUF[writeCount].key=sendPairs[i-1][j].key;
      writeIndex=sendPairs[i-1][j].index;
      memcpy(&writeBUF[writeCount].record,&BUF[writeIndex].record,sizeof(char)*REC_SIZE);
      writeCount+=1;
    }
  }
  fwrite(writeBUF, sizeof(tuple), BUF_SIZE, outputFile);
  fclose(outputFile);
  munmap(BUF, sizeof(tuple) * BUF_SIZE);
  close(fd);
  gettimeofday(&endTime, NULL);
  timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Master totally uses time :%lf s!\n", timeUsed / 1000000);
  for(int i=0;i<THREAD_NUMBER-1;i++){
    free(sendPairs[i]);
    sendPairs[i]=NULL;
  }
  free(writeBUF);
  writeBUF = NULL;
}

void worker() {
  struct timeval startTime, endTime;
  gettimeofday(&startTime, NULL);
  unsigned long recvCount = 0ul;
  int rank = 0, recvEndflag = 0, recvSingleflag = 0;
  MPI_Status status;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  printf("Worker %d start receiving!\n", rank);
  tuple *BUF = (tuple *) malloc(sizeof(tuple) * BUF_SIZE);
  memset(BUF, 0, sizeof(tuple) * BUF_SIZE);
  while (recvEndflag == 0) {
    MPI_Iprobe(0, 0, MPI_COMM_WORLD, &recvSingleflag, &status);
    int recvBytecount = 0;
    MPI_Get_count(&status, MPI_CHAR, &recvBytecount);
    if (recvSingleflag == 1) {
      if (recvBytecount > 1) {
        MPI_Recv(BUF + recvCount, sizeof(tuple), MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        recvCount += 1;
        recvSingleflag = 0;
      } else {
        recvEndflag = 1;
      }
    }

  }
  printf("Worker %d receives %lu tuples, and start sending back!\n", rank, recvCount);
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Worker %d receiving uses time :%lf s!\n", rank, timeUsed / 1000000);
  fastSort(BUF, 0ul, recvCount - 1ul);
  if (recvCount > 0) {
    MPI_Send(&recvCount, 1, MPI_UNSIGNED_LONG, 0, 1, MPI_COMM_WORLD);
    MPI_Send(BUF, sizeof(tuple) * recvCount, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
  }
  free(BUF);
  BUF = NULL;
  gettimeofday(&endTime, NULL);
  timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Worker %d totally uses time :%lf s!\n", rank, timeUsed / 1000000);
}

void worker2() {
  struct timeval startTime, endTime;
  gettimeofday(&startTime, NULL);
  unsigned long recvCount = 0ul;
  int rank = 0, recvEndflag = 0, recvSingleflag = 0;
  MPI_Status status;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  //printf("Worker %d start receiving!\n", rank);
  tuple *BUF = (tuple *) malloc(sizeof(tuple) * BUF_SIZE);
  memset(BUF, 0, sizeof(tuple) * BUF_SIZE);
  while (recvEndflag == 0) {
    MPI_Iprobe(0, 0, MPI_COMM_WORLD, &recvSingleflag, &status);
    int recvBytecount = 0;
    MPI_Get_count(&status, MPI_CHAR, &recvBytecount);
    if (recvSingleflag == 1) {
      if (recvBytecount > 1) {
        MPI_Recv(BUF + recvCount, sizeof(tuple), MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        recvCount += 1;
        recvSingleflag = 0;
      } else {
        recvEndflag = 1;
      }
    }

  }
  printf("Worker %d receives %lu tuples, and start sending back!\n", rank, recvCount);
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Worker %d receiving uses time :%lf s!\n", rank, timeUsed / 1000000);
  struct timeval midTime;
  gettimeofday(&midTime, NULL);
  fastSort(BUF, 0ul, recvCount - 1ul);
  gettimeofday(&endTime, NULL);
  timeUsed = 1000000 * (endTime.tv_sec - midTime.tv_sec) + endTime.tv_usec - midTime.tv_usec;
  printf("Worker %d sorting uses time :%lf s!\n", rank, timeUsed / 1000000);
  int packSize = 0;
  MPI_Pack_size(sizeof(tuple) * recvCount, MPI_CHAR, MPI_COMM_WORLD, &packSize);
  void *sendBuf = malloc(sizeof(tuple) * recvCount + MPI_BSEND_OVERHEAD);
  if (recvCount > 0) {
    MPI_Send(&recvCount, 1, MPI_UNSIGNED_LONG, 0, 1, MPI_COMM_WORLD);
    MPI_Buffer_attach(sendBuf, sizeof(tuple) * recvCount + MPI_BSEND_OVERHEAD);
    MPI_Bsend(BUF, sizeof(tuple) * recvCount, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
    MPI_Buffer_detach(&sendBuf, &packSize);
  }
  free(sendBuf);
  sendBuf = NULL;
  free(BUF);
  BUF = NULL;
  gettimeofday(&endTime, NULL);
  timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Worker %d totally uses time :%lf s!\n", rank, timeUsed / 1000000);
}

void splitworker() {
  struct timeval startTime, endTime;
  double timeUsed=0l;
  gettimeofday(&startTime, NULL);
  unsigned long recvCount = 0ul;
  int rank = 0;
  MPI_Status status;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  printf("Worker %d start receiving!\n", rank);
  MPI_Recv(&recvCount, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &status);
  if (recvCount > 0) {
    keyPair *BUF = (keyPair *) malloc(sizeof(keyPair) * recvCount);
    memset(BUF, 0, sizeof(keyPair) * recvCount);
    MPI_Recv(BUF, sizeof(keyPair) * recvCount, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
    printf("Worker %d receives %lu keys!\n", rank, recvCount);
    keyFastSort(BUF, 0ul, recvCount - 1ul);
    gettimeofday(&endTime, NULL);
    timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
    printf("Worker %d receiving uses time :%lf s!\n", rank, timeUsed / 1000000);
    MPI_Send(BUF, sizeof(keyPair) * recvCount, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    free(BUF);
    BUF = NULL;
  }
  gettimeofday(&endTime, NULL);
  timeUsed = 1000000 * (endTime.tv_sec - startTime.tv_sec) + endTime.tv_usec - startTime.tv_usec;
  printf("Worker %d totally uses time :%lf s!\n", rank, timeUsed / 1000000);
}

int main(int argc, char **argv) {
  init(argc, argv);
  int rank = 0;
  MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (rank == 0)
    splitmaster();
  else
    splitworker();
  MPI_Finalize();
  printf("Bye!\n");
}//
// Created by DP on 2024/11/25.
//
