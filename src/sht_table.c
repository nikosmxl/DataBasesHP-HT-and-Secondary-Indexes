#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int hash(char* s, int buckets);

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
    if( BF_CreateFile(sfileName) != BF_OK ){
        return -1;
    }
    int file_desc;
    if( BF_OpenFile(sfileName,&file_desc) != BF_OK ){
        return -1;
    }

    BF_Block* block;
    
    BF_Block_Init(&block);
    
    if (BF_AllocateBlock(file_desc, block) != BF_OK){
        return -1;
    }

    SHT_info shi;
    shi.file_desc = file_desc;
    shi.buckets = buckets;
    shi.numofpairs_allowed = (BF_BLOCK_SIZE - sizeof(SHT_block_info))/sizeof(Pair);
    shi.hp_or_ht_or_sht = 2;

    SHT_block_info shbi;
    shbi.number_of_pairs = 0;
    shbi.next = NULL;

    void* dat = BF_Block_GetData(block);
    memcpy(dat,&shi,sizeof(SHT_info));
    memcpy(dat + sizeof(SHT_info),&shbi,sizeof(SHT_block_info));
    BF_Block_SetDirty(block);

    // Initializing every bucket of the file
    for(int i = 1 ; i <= shi.buckets ; i++){
        BF_Block* block2;
        
        BF_Block_Init(&block2);
        
        if (BF_AllocateBlock(file_desc, block2) != BF_OK){
            return -1;
        }
        
        SHT_block_info shbi2;
        shbi2.number_of_pairs = 0;
        shbi2.next = NULL;

        void* dat2 = BF_Block_GetData(block2);
        memcpy(dat2 + sizeof(Pair)*shi.numofpairs_allowed, &shbi2, sizeof(SHT_block_info));
        BF_Block_SetDirty(block2);
    }

    if (BF_CloseFile(file_desc) != BF_OK){
        return -1;
    }

    return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
    int file_desc;
    if (BF_OpenFile(indexName, &file_desc) != BF_OK){
        return NULL;
    }

    BF_Block* curr_block;
    
    BF_Block_Init(&curr_block);
    
    if (BF_GetBlock(file_desc, 0, curr_block) != BF_OK){
        return NULL;
    }

    void* for_return = BF_Block_GetData(curr_block);

    SHT_info* tochange = for_return;
    tochange->file_desc = file_desc;                // Updating the file_desc of the first block with the new file_desc

    BF_Block_SetDirty(curr_block);

    SHT_info* info = malloc(sizeof(SHT_info));
    memcpy(info, for_return, sizeof(SHT_info));     // Creating a copy of file's HT_info to return

    if (info->hp_or_ht_or_sht != 2){
        free(info);
        return NULL;
    }

    if (BF_UnpinBlock(curr_block) != BF_OK){
        return NULL;
    }
    
    return info;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
    int* last = malloc(sizeof(int));
    if(BF_GetBlockCounter(SHT_info->file_desc, last) != BF_OK){
        return -1;
    }

    for (int i = 0 ; i < *last ; i++){
        BF_Block* block;

        BF_Block_Init(&block);

        BF_GetBlock(SHT_info->file_desc, i, block);

        void* blockdata = BF_Block_GetData(block);

        SHT_block_info* shbi = malloc(sizeof(SHT_block_info));
        memcpy(shbi, blockdata + sizeof(Pair)*SHT_info->numofpairs_allowed, sizeof(SHT_block_info));

        // In case it is an overflown bucket
        while(shbi->next != NULL){
            BF_Block* next_block;

            BF_Block_Init(&next_block);

            next_block = shbi->next;

            void* blockdata2 = BF_Block_GetData(next_block);
            memcpy(shbi, blockdata2 + sizeof(Pair)*SHT_info->numofpairs_allowed, sizeof(SHT_block_info));

            BF_Block_Destroy(&block);

            block = next_block;
        }

        free(shbi);
        BF_Block_Destroy(&block);
    }

    if (BF_CloseFile(SHT_info->file_desc) != BF_OK){
        return -1;
    }

    free(last);
    return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
    Pair pair;
    memcpy(pair.name, record.name, strlen(record.name) + 1);
    pair.blocknum = block_id;

    BF_Block* block_to_insert;

    BF_Block_Init(&block_to_insert);

    if (BF_GetBlock(sht_info->file_desc, hash(record.name, sht_info->buckets), block_to_insert) != BF_OK){
        return -1;
    }

    void* blocktoinsert_data = BF_Block_GetData(block_to_insert);

    SHT_block_info* shbi = malloc(sizeof(SHT_block_info));
    memcpy(shbi, blocktoinsert_data + sizeof(Pair)*sht_info->numofpairs_allowed, sizeof(SHT_block_info));

    // In case it is an overflown bucket
    while (shbi->next != NULL){
        blocktoinsert_data = BF_Block_GetData(shbi->next);
        memcpy(shbi, blocktoinsert_data + sizeof(Pair)*sht_info->numofpairs_allowed, sizeof(SHT_block_info));
    }

    if (shbi->number_of_pairs < sht_info->numofpairs_allowed){          // If block is not full

        memcpy(blocktoinsert_data + sizeof(Pair)*shbi->number_of_pairs++, &pair, sizeof(Pair));

        memcpy(blocktoinsert_data + sizeof(Pair)*sht_info->numofpairs_allowed, shbi, sizeof(SHT_block_info));

        BF_Block_SetDirty(block_to_insert);

        if (BF_UnpinBlock(block_to_insert) != BF_OK){
            return -1;
        }

        free(shbi);
        return 0;
    }
    
    //If block is full
    BF_Block* new_block;

    BF_Block_Init(&new_block);

    // Allocating a new block in which we will save the record
    if (BF_AllocateBlock(sht_info->file_desc, new_block) != BF_OK){
        return -1;
    }

    SHT_block_info* new_block_info = malloc(sizeof(SHT_block_info));
    new_block_info->number_of_pairs = 1;
    new_block_info->next = NULL;
    
    void* dat = BF_Block_GetData(new_block);
    memcpy(dat, &pair, sizeof(Pair));
    memcpy(dat + sht_info->numofpairs_allowed*sizeof(Pair), new_block_info, sizeof(SHT_block_info));
    BF_Block_SetDirty(new_block);

    shbi->next = new_block;                 // Updating previous block's next pointer

    memcpy(blocktoinsert_data + sizeof(Pair)*sht_info->numofpairs_allowed, shbi, sizeof(SHT_block_info));
    BF_Block_SetDirty(block_to_insert);

    if (BF_UnpinBlock(new_block) != BF_OK){
        return -1;
    }

    if (BF_UnpinBlock(block_to_insert) != BF_OK){
        return -1;
    }

    free(shbi);
    free(new_block_info);
    return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
    void* ht_blockdata;
    int counter = 0;
    
    BF_Block* block;

    BF_Block_Init(&block);

    if (BF_GetBlock(sht_info->file_desc, hash(name, sht_info->buckets), block) != BF_OK){
        return -1;
    }

    void* blockdata = BF_Block_GetData(block);

    SHT_block_info* shbi = malloc(sizeof(SHT_block_info));
    memcpy(shbi, blockdata + sizeof(Pair)*sht_info->numofpairs_allowed, sizeof(SHT_block_info));

    // Checking every record of current block
    for (int i = 0 ; i < shbi->number_of_pairs ; i++){
        Pair* pair = malloc(sizeof(Pair));
        memcpy(pair, blockdata + i*sizeof(Pair), sizeof(Pair));
        
        if (strcmp(pair->name, name) != 0){
            free(pair);
            continue;
        }

        BF_Block* ht_block;

        BF_Block_Init(&ht_block);

        if (BF_GetBlock(ht_info->file_desc, pair->blocknum, ht_block) != BF_OK){
            return -1;
        }

        ht_blockdata = BF_Block_GetData(ht_block);

        HT_block_info* hbi = malloc(sizeof(HT_block_info));
        memcpy(hbi, ht_blockdata + sizeof(Record)*ht_info->numofrecs_allowed, sizeof(HT_block_info));

        for (int j = 0 ; j < hbi->number_of_records ; j++){
            Record* rec = malloc(sizeof(Record));
            memcpy(rec, ht_blockdata + j*sizeof(Record), sizeof(Record));

            if (strcmp(rec->name, name) != 0){
                free(rec);
                continue;
            } 

            printRecord(*rec);
            free(rec);
        }

        free(pair);
        free(hbi);
        counter++;
    }

    // In case it is an overflown bucket
    while (shbi->next != NULL){
        
        blockdata = BF_Block_GetData(shbi->next);
        memcpy(shbi, blockdata + sizeof(Pair)*sht_info->numofpairs_allowed, sizeof(SHT_block_info));

        for (int i = 0 ; i < shbi->number_of_pairs ; i++){
            Pair* pair = malloc(sizeof(Pair));
            memcpy(pair, blockdata + i*sizeof(Pair), sizeof(Pair));

            if (strcmp(pair->name, name) != 0){
                free(pair);
                continue;
            }

            BF_Block* ht_block;

            BF_Block_Init(&ht_block);

            if (BF_GetBlock(ht_info->file_desc, pair->blocknum, ht_block) != BF_OK){
                return -1;
            }

            ht_blockdata = BF_Block_GetData(ht_block);

            HT_block_info* hbi = malloc(sizeof(HT_block_info));
            memcpy(hbi, ht_blockdata + sizeof(Record)*ht_info->numofrecs_allowed, sizeof(HT_block_info));

            for (int j = 0 ; j < hbi->number_of_records ; j++){
                Record* rec = malloc(sizeof(Record));
                memcpy(rec, ht_blockdata + i*sizeof(Record), sizeof(Record));

                if (strcmp(rec->name, name) != 0){
                    free(rec);
                    continue;
                } 

                printRecord(*rec);
                free(rec);
            }

            free(hbi);
            counter++;
        }
    }

    if (BF_UnpinBlock(block) != BF_OK){
        return -1;
    }
    
    free(shbi);
    return counter;
}

int hash(char* s, int buckets){
  int g = 31;
  long h = 0;
  
  for (int i = 0 ; i < strlen(s) ; i++){    
    h += g * h + (int)s[i];
  }

  return h % buckets + 1;
}

