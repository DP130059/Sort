#include<stdlib.h>
#include<string.h>
#include<stdio.h>
#include<stdint.h>
#include<time.h>
#include<sys/time.h>
#include<memory.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <cjson/cJSON.h>

#define KEY_SIZE 8ul
#define REC_SIZE 56ul
#define GB 1ul << 24ul

typedef struct {
  uint64_t key;
  char record[REC_SIZE];
} tuple;

char resultFileAddr[128], configFileAddr[128] = "./config.json";
uint32_t workerSize = 0u;
uint64_t dataSize = 0ul, tupleSingleCount = 0ul, tupleTotalCount = 0ul;

uint64_t count_files_in_dir(const char *path)
{
  DIR           *d;
  struct dirent *ent;
  struct stat    st;
  char           full[4096];
  uint64_t           n = 0;

  d = opendir(path);
  if (!d) return -1;

  while ((ent = readdir(d))) {
    /* 跳过 . 和 .. */
    if (ent->d_name[0] == '.' &&
        (ent->d_name[1] == '\0' ||
         (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
      continue;

    /* 拼接完整路径 */
    int len = snprintf(full, sizeof(full), "%s/%s", path, ent->d_name);
    if (len >= (int)sizeof(full)) continue;   /* 路径太长，跳过 */

    if (stat(full, &st) == 0 && S_ISREG(st.st_mode))
      ++n;               /* 只计普通文件 */
  }
  closedir(d);
  return n;
}

void parseConfig() {
  FILE *configFile = fopen(configFileAddr, "r");
  if (configFile == NULL) {
    perror("Error opening config file\n");
  }
  fseek(configFile, 0,SEEK_END);
  uint64_t fileSize = ftell(configFile);
  rewind(configFile);
  char *buffer = (char *) malloc(fileSize + 1);
  if (buffer == NULL) {
    perror("Error allocating memory\n");
  }
  fread(buffer, 1, fileSize, configFile);
  fclose(configFile);
  cJSON *json = cJSON_Parse(buffer);
  if (json == NULL) {
    perror("Error decoding JSON\n");
  }
  free(buffer);
  buffer = NULL;
  cJSON *cjsworkerSize = cJSON_GetObjectItem(json, "workerSize");
  workerSize = (uint32_t) (cjsworkerSize->valueint);
  cJSON *cjsdataSize = cJSON_GetObjectItem(json, "dataSize");
  dataSize = ((uint64_t) cjsdataSize->valueint) * GB ;
  tupleSingleCount = dataSize / sizeof(tuple);
  tupleTotalCount = tupleSingleCount * workerSize;
  cJSON *cjsResultAddr=cJSON_GetObjectItem(json, "outputFileAddr");
  strcpy(resultFileAddr, cjsResultAddr->valuestring);

}

char check(tuple *BUF,uint64_t bufSize,uint64_t lastBatchKey) {
  if (lastBatchKey>BUF[0].key) {
    return -1;
  }
  for (uint64_t i = 0; i < bufSize - 1; i++) {
    if (BUF[i].key > BUF[i + 1].key) {
      return -1;
    }
  }
  return 1;
}


int main(int argc, char *argv[]) {
  parseConfig();
  uint64_t batchCount = count_files_in_dir(resultFileAddr);
  char checkFileAddr[128],batchCountString[128];
  uint64_t lastBatchKey=0ul;
  //printf("\n\nFUCKKKKKKKK\n");
  for (uint64_t i=0;i<batchCount;i++) {
    memset(checkFileAddr, 0, 128);
    memset(batchCountString, 0, 128);
    strcpy(checkFileAddr, resultFileAddr);
    snprintf(batchCountString, sizeof(batchCountString), "%lu", i);
    strcat(checkFileAddr, batchCountString);
    FILE *file=fopen(checkFileAddr, "rb");
    if (file == NULL) {
      perror("Error opening result file for reading!\n");
    }
    fseek(file, 0, SEEK_END);
    uint64_t fileSize = ftell(file);
    rewind(file);
    uint64_t batchTupleCount=fileSize / sizeof(tuple);
    tuple *BUF = (tuple *) malloc(batchTupleCount * sizeof(tuple));
    if (BUF == NULL) {
      perror("Error allocating memory\n");
    }
    uint64_t readCount=fread(BUF, sizeof(tuple), batchTupleCount, file);
    if (readCount != batchTupleCount) {
      perror("Error reading input file!\n");
    }
    fclose(file);
    if (check(BUF, batchTupleCount, lastBatchKey)<0) {
      printf("Fail!!\n");
      free(BUF);
      BUF = NULL;
      return 0;
    }
    lastBatchKey=BUF[readCount-1].key;
    free(BUF);
    BUF=NULL;
  }
  printf("Success!!\n");
}
