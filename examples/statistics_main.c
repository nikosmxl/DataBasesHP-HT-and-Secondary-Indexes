#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"
#include "hp_file.h"
#include "statistics.h"

#define RECORDS_NUM 30 // you can change it if you want
#define FILE_NAME_HP "data_hp.db"
#define FILE_NAME_HT "data_ht.db"
#define INDEX_NAME "index.db"

int main(){
    BF_Init(LRU);
    int cur = time(NULL);
    srand((unsigned int) cur);

    HP_CreateFile(FILE_NAME_HP);
  
    HP_info* info_hp = HP_OpenFile(FILE_NAME_HP);

    HT_CreateFile(FILE_NAME_HT,10);
    SHT_CreateSecondaryIndex(INDEX_NAME,10,FILE_NAME_HT);
    HT_info* info_ht = HT_OpenFile(FILE_NAME_HT);
    SHT_info* index_info = SHT_OpenSecondaryIndex(INDEX_NAME);

    Record record=randomRecord();
    char searchName[15];
    strcpy(searchName, record.name);;
    srand(12569874);

    printf("Insert Entries\n");
    for (int id = 0; id < RECORDS_NUM; ++id) {
        record = randomRecord();
        HP_InsertEntry(info_hp, record);
        int block_id = HT_InsertEntry(info_ht, record);
        SHT_SecondaryInsertEntry(index_info, record, block_id);
    }

    // Statistics for all of the files
    HashStatistics(FILE_NAME_HP);
    HashStatistics(FILE_NAME_HT);
    HashStatistics(INDEX_NAME);

    SHT_CloseSecondaryIndex(index_info);
    HT_CloseFile(info_ht);
    HP_CloseFile(info_hp);

    BF_Close();
}
