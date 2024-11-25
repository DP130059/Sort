#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <sys/time.h>

#define REC_SIZE 56
typedef struct {
  unsigned long key;
  unsigned char record[REC_SIZE];
} tuple;

char inputFileaddr[128] = "./Tuple";

int compare_keys(const void *a, const void *b) {
  unsigned long key1 = *(unsigned long *) a;
  unsigned long key2 = *(unsigned long *) b;
  return (key1 > key2) - (key1 < key2);
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


int main(int argc, char *argv[]) {
  struct timeval beginTime, endTime;
  gettimeofday(&beginTime, NULL);
  MPI_Init(&argc, &argv);

  int world_rank, world_size;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  strcat(inputFileaddr, argv[1]);
  unsigned long DATA_SIZE = atol(argv[1]);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  unsigned long M = 4ul;
  unsigned long N = DATA_SIZE * (1ul << 26ul);
  unsigned long pmin = 4ul;
  unsigned long pmax = 1ul << 15ul;
  unsigned long p = pmin;
  unsigned long *pivot_keys = NULL;
  double s = 1.05f;
  tuple *tuples = NULL;

  MPI_Status statuses[world_size];
  unsigned long bar = s * N / M;
  if (world_rank == 0) {
    printf("bar:%lu\n", bar);
  }

  // 打开文件读取
  FILE *file = fopen(inputFileaddr, "rb");
  if (file == NULL) {
    perror("Unable to open file for reading");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  // 分配内存读取所有 tuples
  fseek(file, 0, SEEK_END);
  unsigned long file_size = ftell(file);
  fseek(file, 0, SEEK_SET);
  unsigned long tuple_count = file_size / sizeof(tuple);
  tuples = (tuple *) malloc(file_size);
  fread(tuples, sizeof(tuple), tuple_count, file);
  fclose(file);

  int found = 0;

  srand((unsigned) time(NULL));
  while (p <= pmax && !found) {
    unsigned long total_keys_count = p * (unsigned long) world_size;
    unsigned long *keys = (unsigned long *) malloc(p * sizeof(unsigned long));
    memset(keys, 0ul, sizeof(unsigned long) * p);
    unsigned long random_index = 0ul;
    unsigned long *all_keys = (unsigned long *) malloc(total_keys_count * sizeof(unsigned long));
    memset(all_keys, 0ul, sizeof(unsigned long) * total_keys_count);
    unsigned long *counts = (unsigned long *) malloc((total_keys_count + 1ul) * sizeof(unsigned long));
    memset(counts, 0ul, (total_keys_count + 1ul) * sizeof(unsigned long));
    unsigned long *all_counts = (unsigned long *) malloc((total_keys_count + 1ul) * world_size * sizeof(unsigned long));
    memset(all_counts, 0ul, (total_keys_count + 1ul) * world_size * sizeof(unsigned long));
    unsigned long final_counts[total_keys_count + 1ul];
    memset(&final_counts, 0ul, (total_keys_count + 1ul) * sizeof(unsigned long));
    pivot_keys = (unsigned long *) malloc(total_keys_count * sizeof(unsigned long));
    memset(pivot_keys, 0ul, total_keys_count * sizeof(unsigned long));
    unsigned long *pivot = (unsigned long *) malloc(total_keys_count * sizeof(unsigned long));
    memset(pivot, 0ul, total_keys_count * sizeof(unsigned long));

    // 随机选择 p 个 key
    for (unsigned long i = 0ul; i < p; i++) {
      random_index = rand() % tuple_count;
      keys[i] = tuples[random_index].key;
    }
    if (world_rank == 0) {
      printf("p:%lu\n", p);
    }

    // 打印每个进程选取的 keys
    /*
    printf("Process %d selected keys: \n", world_rank);
    for (int i = 0; i < p; i++) {
        printf("%lu\n", keys[i]);
    }
    */

    // 广播每个进程的 keys
    //memset(all_keys,0ul,sizeof(unsigned long)*total_keys_count);
    MPI_Allgather(keys, p, MPI_UNSIGNED_LONG, all_keys, p, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

    // 对所有的 keys 进行排序
    qsort(all_keys, p * world_size, sizeof(unsigned long), compare_keys);

    // 打印排序后的 keys（仅进程0）

    /*if (world_rank == 0) {
        printf("Sorted keys: \n");
        for (int i = 0; i < p * world_size; i++) {
            printf("%lu\n", all_keys[i]);
        }
    }*/

    // 统计每个 key 划分的 tuple 数量

    // memset(counts, 0, (p * world_size + 1) * sizeof(unsigned long));
    for (unsigned long i = 0ul; i < tuple_count; i++) {
      unsigned long range_index = find_range(all_keys, p * world_size, tuples[i].key);
      counts[range_index] += 1ul;
    }

    // 打印统计结果
    /*
    printf("Process %d tuple counts: \n", world_rank);
    for (int i = 0; i < p * world_size + 1; i++) {
        printf("Range %d: %d tuples\n", i, counts[i]);
    }
    */

    // 广播每个进程的 tuple counts 并汇总
    //memset(all_counts,0ul,(p * world_size + 1ul) * world_size * sizeof(unsigned long));
    MPI_Allgather(counts,
                  p * world_size + 1,
                  MPI_UNSIGNED_LONG,
                  all_counts,
                  p * world_size + 1,
                  MPI_UNSIGNED_LONG,
                  MPI_COMM_WORLD);

    // 汇总所有进程的统计数据

    memset(&final_counts, 0ul, (total_keys_count + 1ul) * sizeof(unsigned long));
    for (unsigned long i = 0ul; i < world_size; i++) {
      for (unsigned long j = 0ul; j < total_keys_count + 1ul; j++) {
        final_counts[j] += all_counts[i * (total_keys_count + 1ul) + j];
      }
    }

    // 打印汇总的统计结果（仅进程0）

    /*if (world_rank == 0) {
         printf("Final tuple counts across all processes: \n");
         for (int i = 0; i < p * world_size + 1; i++) {
             printf("Range %d: %d tuples\n", i, final_counts[i]);
         }
     }*/
    // 判断能否找到M-1个枢轴Key
    found = 1;
    unsigned long sum = 0ul;

    unsigned long j = 0ul;
    for (unsigned long i = 0ul; i < total_keys_count + 1ul; i++) {
      if (final_counts[i] > bar) {
        found = 0;
        break;
      }
      if ((sum + final_counts[i]) > bar) {
        pivot_keys[j] = all_keys[i - 1];
        pivot[j] = sum;
        sum = 0;
        sum += final_counts[i];
        j += 1ul;
      } else {
        sum += final_counts[i];
      }
    }
    //printf("j: %lu\n",j);
    if (j != (M - 1)) {
      found = 0;
    } else {
      pivot[j] = sum;
    }

    if (!found) {
      p *= 2;
    } else {
      if (world_rank == 0) {
        for (unsigned long i = 0ul; i <= j; i++) {
          printf("Pivot range %lu: %lu tuples\n", i, pivot[i]);
        }
        for (unsigned long i = 0ul; i < j; i++) {
          printf("Pivot key %lu: %lu \n", i, pivot_keys[i]);
        }
      }
    }

    free(keys);
    free(all_keys);
    free(counts);
    free(all_counts);
    // free(pivot_keys);
    free(pivot);
    //free(final_counts);

  }


  // 统计发送给每个进程的 tuple 数量
  unsigned long *send_counts = (unsigned long *) malloc(M * sizeof(unsigned long));
  memset(send_counts, 0, M * sizeof(unsigned long));
  for (unsigned long i = 0ul; i < tuple_count; i++) {
    unsigned long process_number = find_range(pivot_keys, M - 1ul, tuples[i].key);
    //if (process_number!=world_rank){
    send_counts[process_number] += 1ul;
    //}

  }
  //int local_counts = send_counts[world_rank];
  //send_counts[world_rank]=0;

  // 统计每个进程的接收tuple数量
  unsigned long *all_recv_counts = (unsigned long *) malloc(M * world_size * sizeof(unsigned long));
  MPI_Allgather(send_counts, M, MPI_UNSIGNED_LONG, all_recv_counts, M, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
  unsigned long *recv_counts = (unsigned long *) malloc(M * sizeof(unsigned long));
  memset(recv_counts, 0, M * sizeof(unsigned long));
  for (unsigned long i = 0ul; i < M * world_size; i++) {
    //printf("process %d to process %d : %d\n",(i/M),(i%M),all_recv_counts[i]);
    if ((i % M) == world_rank && (i / M) != world_rank) {
      recv_counts[(i / M)] = all_recv_counts[i];
      //printf("process %d to process %d : %d\n",(i/M),(i%M),recv_counts[(i/M)]);
    }
  }

  // 创建非阻塞通信请求数组
  MPI_Request *send_requests = (MPI_Request *) malloc((world_size) * sizeof(MPI_Request));
  MPI_Request *recv_requests = (MPI_Request *) malloc((world_size) * sizeof(MPI_Request));
  send_requests[world_rank] = MPI_REQUEST_NULL;
  recv_requests[world_rank] = MPI_REQUEST_NULL;

  // 分配发送和接收缓冲区
  tuple **send_buffers = (tuple **) malloc((world_size) * sizeof(tuple *));
  tuple **recv_buffers = (tuple **) malloc((world_size) * sizeof(tuple *));
  for (unsigned long i = 0ul; i < world_size; i++) {
    if (i != world_rank) {
      send_buffers[i] = (tuple *) malloc(send_counts[i] * sizeof(tuple));
      recv_buffers[i] = (tuple *) malloc(recv_counts[i] * sizeof(tuple));
    } else {
      send_buffers[i] = NULL;
      recv_buffers[i] = NULL;
    }
  }

  // 填充发送缓冲区
  tuple *local_data = (tuple *) malloc(send_counts[world_rank] * sizeof(tuple));
  unsigned long j = 0;
  unsigned long *send_offsets = (unsigned long *) calloc(world_size, sizeof(unsigned long));
  for (unsigned long i = 0ul; i < tuple_count; i++) {
    unsigned long process_number = find_range(pivot_keys, M - 1ul, tuples[i].key);
    if (process_number != world_rank) {
      send_buffers[process_number][send_offsets[process_number]++] = tuples[i];
    } else {
      local_data[j] = tuples[i];
      j++;
    }
  }

  // 非阻塞发送
  for (unsigned long i = 0ul; i < world_size; i++) {
    if (i != world_rank) {
      MPI_Isend(send_buffers[i], send_counts[i] * sizeof(tuple), MPI_BYTE, i, 0, MPI_COMM_WORLD, &send_requests[i]);
    } else {
      send_requests[i] = MPI_REQUEST_NULL;
    }
  }

  // 非阻塞接收
  for (unsigned long i = 0ul; i < world_size; i++) {
    if (i != world_rank) {
      MPI_Irecv(recv_buffers[i], recv_counts[i] * sizeof(tuple), MPI_BYTE, i, 0, MPI_COMM_WORLD, &recv_requests[i]);
    } else {
      recv_requests[i] = MPI_REQUEST_NULL;
    }
  }


  // 等待所有发送和接收操作完成
  MPI_Waitall(world_size, send_requests, statuses);
  MPI_Waitall(world_size, recv_requests, statuses);

  // 计算合并后所有 tuple 的总数
  unsigned long local_data_count = send_counts[world_rank];
  unsigned long total_tuples_count = local_data_count;

  for (unsigned long i = 0ul; i < world_size; i++) {
    if (i != world_rank) {
      total_tuples_count += recv_counts[i];
    }
  }

  // 分配新的内存来存储合并后的数据
  tuple *merged_data = (tuple *) malloc(total_tuples_count * sizeof(tuple));

  // 将 local_data 拷贝到 merged_data 中
  memcpy(merged_data, local_data, local_data_count * sizeof(tuple));

  // 将 recv_buffers 中的数据追加到 merged_data 中
  unsigned long offset = local_data_count;
  for (unsigned long i = 0ul; i < world_size; i++) {
    if (i != world_rank) {
      memcpy(merged_data + offset, recv_buffers[i], recv_counts[i] * sizeof(tuple));
      offset += recv_counts[i];
    }
  }

  // 统计合并后的元组数量
  printf("Process %d: Total number of tuples after merging: %lu\n", world_rank, total_tuples_count);



  // 对 merged_data 按照 key 进行排序
  qsort(merged_data, total_tuples_count, sizeof(tuple), compare_keys);

  // 写入新的文件中
  char output_filename[128];
  sprintf(output_filename, "./SortedTuple%lu_%d", DATA_SIZE, world_rank);

  FILE *output_file = fopen(output_filename, "wb");
  if (output_file == NULL) {
    perror("Unable to open output file for writing");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  fwrite(merged_data, sizeof(tuple), total_tuples_count, output_file);
  fclose(output_file);

  // 清理内存
  free(merged_data);
  free(local_data);
  free(send_counts);
  free(send_buffers);
  free(recv_buffers);
  free(send_requests);
  free(recv_requests);
  free(tuples);
  free(send_offsets);
  free(pivot_keys);

  MPI_Finalize();
  gettimeofday(&endTime, NULL);
  double timeUsed = 1000000 * (endTime.tv_sec - beginTime.tv_sec) + endTime.tv_usec - beginTime.tv_usec;
  printf("Worker %d totally uses time :%lf s!\n", world_rank, timeUsed / 1000000);
  return 0;
}

//
// Created by DP on 2024/11/25.
//
