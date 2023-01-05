#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

int Hash(HT_info* hi, int id);

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


int HT_CreateFile(char *fileName,  int buckets){
    if( BF_CreateFile(fileName) != BF_OK ){
        return -1;
    }
    int file_desc;
    if( BF_OpenFile(fileName,&file_desc) != BF_OK ){
        return -1;
    }
    
    BF_Block* block;
    
    BF_Block_Init(&block);
    
    if (BF_AllocateBlock(file_desc, block) != BF_OK){
        return -1;
    }
    
    HT_info hi;
    hi.file_desc = file_desc;
    hi.numofrecs_allowed = (BF_BLOCK_SIZE - sizeof(HT_block_info))/sizeof(Record);
    hi.hp_or_ht_or_sht = 1;
    hi.numBuckets = buckets;

    HT_block_info hbi;
    hbi.number_of_records = 0;
    hbi.next = NULL;
    hbi.id = 0;
    
    void* dat = BF_Block_GetData(block);
    memcpy(dat,&hi,sizeof(HT_info)*sizeof(char));
    memcpy(dat+sizeof(HT_info),&hbi,sizeof(HT_block_info)*sizeof(char));
    BF_Block_SetDirty(block);

    // Initializing every bucket of the file
    for(int i = 1 ; i <= hi.numBuckets ; i++){
        BF_Block* block2;
        
        BF_Block_Init(&block2);
        
        if (BF_AllocateBlock(file_desc, block2) != BF_OK){
            return -1;
        }
        
        HT_block_info hbi2;
        hbi2.number_of_records = 0;
        hbi2.next = NULL;
        hbi2.id = i;

        void* dat2 = BF_Block_GetData(block2);
        memcpy(dat2+sizeof(Record)*hi.numofrecs_allowed,&hbi2,sizeof(HT_block_info)*sizeof(char));
        BF_Block_SetDirty(block2);
    }

    if (BF_CloseFile(file_desc) != BF_OK){
        return -1;
    }

    return 0;
}

HT_info* HT_OpenFile(char *fileName){
    int file_desc;
    if (BF_OpenFile(fileName, &file_desc) != BF_OK){
        return NULL;
    }

    BF_Block* curr_block;
    
    BF_Block_Init(&curr_block);
    
    if (BF_GetBlock(file_desc, 0, curr_block) != BF_OK){
        return NULL;
    }

    void* for_return = BF_Block_GetData(curr_block);

    HT_info* tochange = for_return;
    tochange->file_desc = file_desc;            // Updating the file_desc of the first block with the new file_desc

    BF_Block_SetDirty(curr_block);

    HT_info* info = malloc(sizeof(HT_info));
    memcpy(info, for_return, sizeof(HT_info));  // Creating a copy of file's HT_info to return

    if (info->hp_or_ht_or_sht != 1){
        free(info);
        return NULL;
    }

    if (BF_UnpinBlock(curr_block) != BF_OK){
        return NULL;
    }
    
    return info;
}


int HT_CloseFile( HT_info* HT_info ){
    int* last = malloc(sizeof(int));
    if(BF_GetBlockCounter(HT_info->file_desc, last) != BF_OK){
        return -1;
    }

    for (int i = 0 ; i < *last ; i++){
        BF_Block* block;

        BF_Block_Init(&block);

        BF_GetBlock(HT_info->file_desc, i, block);

        void* blockdata = BF_Block_GetData(block);

        HT_block_info* hbi = malloc(sizeof(HT_block_info));
        memcpy(hbi, blockdata + sizeof(Record)*HT_info->numofrecs_allowed, sizeof(HT_block_info));

        // In case it is an overflown bucket
        while(hbi->next != NULL){
            BF_Block* next_block;

            BF_Block_Init(&next_block);

            next_block = hbi->next;

            void* blockdata2 = BF_Block_GetData(next_block);
            memcpy(hbi, blockdata2 + sizeof(Record)*HT_info->numofrecs_allowed, sizeof(HT_block_info));

            BF_Block_Destroy(&block);

            block = next_block;
        }

        free(hbi);
        BF_Block_Destroy(&block);
    }

    if (BF_CloseFile(HT_info->file_desc) != BF_OK){
        return -1;
    }

    free(last);
    return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
    BF_Block* block;

    BF_Block_Init(&block);

    BF_ErrorCode error = BF_GetBlock(ht_info->file_desc, Hash(ht_info, record.id), block);
    if (error != BF_OK){
        return error;
    }
    void* blockdata = BF_Block_GetData(block);

    HT_block_info* hbi = malloc(sizeof(HT_block_info));
    memcpy(hbi, blockdata + sizeof(Record)*ht_info->numofrecs_allowed, sizeof(HT_block_info));

    // In case it is an overflown bucket
    while (hbi->next != NULL){
        blockdata = BF_Block_GetData(hbi->next);
        memcpy(hbi, blockdata + sizeof(Record)*ht_info->numofrecs_allowed, sizeof(HT_block_info));
    }

    if (hbi->number_of_records < ht_info->numofrecs_allowed){       // If block is not full

        memcpy(blockdata + sizeof(Record)*hbi->number_of_records++, &record, sizeof(Record));

        memcpy(blockdata + sizeof(Record)*ht_info->numofrecs_allowed, hbi, sizeof(HT_block_info));

        BF_Block_SetDirty(block);

        error = BF_UnpinBlock(block);
        if (error != BF_OK){
            return error;
        }

        int idtoreturn = hbi->id;
        free(hbi);
        return idtoreturn;
    }

    // If block is full
    BF_Block* new_block;

    BF_Block_Init(&new_block);

    // Allocating a new block in which we will save the record
    error = BF_AllocateBlock(ht_info->file_desc, new_block);
    if (error != BF_OK){
        return error;
    }   

    HT_block_info* new_block_info = malloc(sizeof(HT_block_info));
    new_block_info->number_of_records = 1;

    error = BF_GetBlockCounter(ht_info->file_desc, &new_block_info->id);
    if (error != BF_OK){
        return error;
    }
    new_block_info->next = NULL;
    
    void* dat = BF_Block_GetData(new_block);
    memcpy(dat,&record,sizeof(Record));
    memcpy(dat + ht_info->numofrecs_allowed*sizeof(Record), new_block_info, sizeof(HT_block_info));
    BF_Block_SetDirty(new_block);

    hbi->next = new_block;          // Updating previous block's next pointer

    memcpy(blockdata + sizeof(Record)*ht_info->numofrecs_allowed, hbi, sizeof(HT_block_info));

    BF_Block_SetDirty(block);

    error = BF_UnpinBlock(block);
    if (error != BF_OK){
        return error;
    }

    error = BF_UnpinBlock(new_block);
    if (error != BF_OK){
        return error;
    }

    int return_it = new_block_info->id;
    free(hbi);
    free(new_block_info);
    return return_it;
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
    int counter = 0;
    
    BF_Block* block;

    BF_Block_Init(&block);

    if (BF_GetBlock(ht_info->file_desc, Hash(ht_info, *((int*)(value))), block) != BF_OK){
        return -1;
    }

    void* blockdata = BF_Block_GetData(block);

    HT_block_info* hbi = malloc(sizeof(HT_block_info));
    memcpy(hbi, blockdata + sizeof(Record)*ht_info->numofrecs_allowed, sizeof(HT_block_info));

    // Checking every record of current block
    for (int i = 0 ; i < hbi->number_of_records ; i++){
        Record* rec = malloc(sizeof(Record));
        memcpy(rec, blockdata + i*sizeof(Record), sizeof(Record));

        if (rec->id != *((int*)(value))){
            free(rec);
            continue;
        } 

        printRecord(*rec);
        free(rec);
    }

    counter++;

    // In case it is an overflown bucket
    while (hbi->next != NULL){
        
        void* blockdata = BF_Block_GetData(hbi->next);
        memcpy(hbi,blockdata + sizeof(Record)*ht_info->numofrecs_allowed,sizeof(HT_block_info));

        for (int i = 0 ; i < hbi->number_of_records ; i++){
            Record* rec = malloc(sizeof(Record));
            memcpy(rec, blockdata + i*sizeof(Record), sizeof(Record));

            if (rec->id != *((int*)(value))){
                free(rec);
                continue;
            }

            printRecord(*rec);
            free(rec);
        }

        counter++;
    }

    if (BF_UnpinBlock(block) != BF_OK){
        return -1;
    }
    
    free(hbi);
    return counter;
}


int Hash(HT_info* hi, int id){
    return id % hi->numBuckets + 1;
}

