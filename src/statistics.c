#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"
#include "hp_file.h"
#include "statistics.h"

int HashStatistics(char* filename){
    HP_info* info_hp = HP_OpenFile(filename);
    HT_info* info_ht = HT_OpenFile(filename);
    SHT_info* info_sht = SHT_OpenSecondaryIndex(filename);
    int to_return;

    if (info_hp != NULL){
        to_return = HashStatistics_HP(filename, info_hp);
    }
    else if (info_ht != NULL){
        to_return = HashStatistics_HT(filename, info_ht);
    }
    else if (info_sht != NULL){
        to_return = HashStatistics_SHT(filename, info_sht);
    }
    else{
        printf("File not found\n");
        return -1;
    }

    return to_return;
}

int HashStatistics_HP(char* filename, HP_info* info_hp){
    int blocks;
    if (BF_GetBlockCounter(info_hp->file_desc, &blocks) != BF_OK){
        return -1;
    }
    blocks--;
    int min;
    int max;
    double avg;
    int coun = 0;
    for(int i = 1; i <= blocks; i++){
        BF_Block* bl;
        BF_Block_Init(&bl);
        if( BF_GetBlock(info_hp->file_desc,i,bl) != BF_OK ){
            return -1;
        }
        void* blockdata = BF_Block_GetData(bl);

        HP_block_info* hbi = malloc(sizeof(HP_block_info));
        memcpy(hbi,blockdata + sizeof(Record)*info_hp->numofrecs_allowed, sizeof(HP_block_info));
        coun+=hbi->number_of_records;
        if( i == 1 ){
            min = hbi->number_of_records;
            max = min;
        }
        else{
            if( max < hbi->number_of_records ){
                max = hbi->number_of_records;
            }
            if( min > hbi->number_of_records ){
                min = hbi->number_of_records;
            }
        }
    }
    avg = ((double)(coun))/blocks;
    double bl_per_bu = 1;
    int vf[blocks];
    for(int i = 0; i < blocks; i++){
        vf[i] = 0;
    }
    int overflown_bucks = 0;
    printf("Printing HP file's Hashfunction:\n");
    printf("Number of blocks: %d\n", blocks);
    printf("Minimum number of records per bucket: %d\n", min);
    printf("Maximum number of records per bucket: %d\n", max);
    printf("Average number of records per bucket: %f\n", avg);
    printf("Average number of blocks per bucket: %f\n", bl_per_bu);
    printf("Number of overflown buckets: %d\n", overflown_bucks);
    printf("Number of blocks per overflown bucket:\n");
    int i;
    for (i = 0 ; i < blocks ; i++){
        if (i % 5 == 0){
            printf("\n");
        }
        printf("Block %d: %d\t", i + 1, vf[i]);
    }
    if (i % 5 != 1){
        printf("\n");
    }
    printf("\n");

    return 0;
}

int HashStatistics_HT(char* filename, HT_info* info_ht){
    int blocks;
    if (BF_GetBlockCounter(info_ht->file_desc, &blocks) != BF_OK){
        return -1;
    }
    blocks--;

    int min = 100;
    int max = -1;
    double avg;
    int coun = 0;
    int count_perblock = 0;
    int overflown_bucks = 0;

    int vf[info_ht->numBuckets];

    for(int i = 1; i <= info_ht->numBuckets; i++){
        int count_perblock = 0;

        BF_Block* bl;
        BF_Block_Init(&bl);
        if( BF_GetBlock(info_ht->file_desc,i,bl) != BF_OK ){
            return -1;
        }
        void* blockdata = BF_Block_GetData(bl);

        HT_block_info* hbi = malloc(sizeof(HT_block_info));
        memcpy(hbi,blockdata + sizeof(Record)*info_ht->numofrecs_allowed, sizeof(HT_block_info));

        if (hbi->next != NULL) overflown_bucks++;

        vf[i - 1] = 0;
        int j;
        for(j = 0 ; hbi->next != NULL ; j++){
            count_perblock += hbi->number_of_records;
            coun += hbi->number_of_records;

            if (j > 0){
                vf[i - 1] += count_perblock;
            }

            void* blockdata = BF_Block_GetData(hbi->next);
            memcpy(hbi, blockdata + sizeof(Record)*info_ht->numofrecs_allowed, sizeof(HT_block_info));
        }

        coun += hbi->number_of_records;
        count_perblock += hbi->number_of_records;

        if (j > 0){
            vf[i - 1] += count_perblock;
        }

        if (min > count_perblock){
            min = count_perblock;
        }

        if (max < count_perblock){
            max = count_perblock;
        }
    }
    avg = ((double)(coun))/blocks;
    double blocks_per_buck = ((double)(blocks))/ info_ht->numBuckets;

    printf("Printing HT file's Hashfunction:\n");
    printf("Number of blocks: %d\n", blocks);
    printf("Minimum number of records per bucket: %d\n", min);
    printf("Maximum number of records per bucket: %d\n", max);
    printf("Average number of records per bucket: %f\n", avg);
    printf("Average number of blocks per bucket: %f\n", blocks_per_buck);
    printf("Number of overflown buckets: %d\n", overflown_bucks);
    printf("Number of blocks per overflown bucket:\n");
    int i;
    for (i = 0 ; i < info_ht->numBuckets ; i++){
        if (i % 5 == 0){
            printf("\n");
        }
        printf("Block %d: %d\t", i + 1, vf[i]);
    }
    if (i % 5 != 1){
        printf("\n");
    }
    printf("\n");

    return 0;
}

int HashStatistics_SHT(char* filename, SHT_info* info_sht){
    int blocks;
    BF_GetBlockCounter(info_sht->file_desc, &blocks);
    blocks--;

    int min = 100;
    int max = -1;
    double avg;
    int coun = 0;
    int overflown_bucks = 0;

    int vf[info_sht->buckets];

    for(int i = 1; i <= info_sht->buckets; i++){
        int count_perblock = 0;

        BF_Block* bl;
        BF_Block_Init(&bl);
        if( BF_GetBlock(info_sht->file_desc,i,bl) != BF_OK ){
            return -1;
        }
        void* blockdata = BF_Block_GetData(bl);

        SHT_block_info* shbi = malloc(sizeof(SHT_block_info));
        memcpy(shbi,blockdata + sizeof(Pair)*info_sht->numofpairs_allowed, sizeof(SHT_block_info));

        if (shbi->next != NULL) overflown_bucks++;

        vf[i - 1] = 0;
        int j;
        for(j = 0 ; shbi->next != NULL ; j++){
            count_perblock += shbi->number_of_pairs;
            coun += shbi->number_of_pairs;

            if (j > 0){
                vf[i - 1] += count_perblock;
            }

            void* blockdata = BF_Block_GetData(shbi->next);
            memcpy(shbi, blockdata + sizeof(Pair)*info_sht->numofpairs_allowed, sizeof(SHT_block_info));
        }

        coun += shbi->number_of_pairs;
        count_perblock += shbi->number_of_pairs;

        if (j > 0){
            vf[i - 1] += count_perblock;
        }

        if (min > count_perblock){
            min = count_perblock;
        }

        if (max < count_perblock){
            max = count_perblock;
        }
    }
    avg = ((double)(coun))/blocks;
    double blocks_per_buck = ((double)(blocks))/ info_sht->buckets;

    printf("Printing SHT file's Hashfunction:\n");
    printf("Number of blocks: %d\n", blocks);
    printf("Minimum number of records per bucket: %d\n", min);
    printf("Maximum number of records per bucket: %d\n", max);
    printf("Average number of records per bucket: %f\n", avg);
    printf("Average number of blocks per bucket: %f\n", blocks_per_buck);
    printf("Number of overflown buckets: %d\n", overflown_bucks);
    printf("Number of blocks per overflown bucket:\n");
    int i;
    for (i = 0 ; i < info_sht->buckets ; i++){
        if (i % 5 == 0){
            printf("\n");
        }
        printf("Block %d: %d\t", i + 1, vf[i]);
    }
    if (i % 5 != 1){
        printf("\n");
    }
    printf("\n");

    return 0;
}