#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

int HP_CreateFile(char *fileName){
    if( BF_CreateFile(fileName) != BF_OK ){
      return -1;
    }

    int file_desc;
    if( BF_OpenFile(fileName,&file_desc) != BF_OK ){
      return -1;
    }
    
    BF_Block *block;

    BF_Block_Init(&block);

    if (BF_AllocateBlock(file_desc, block) != BF_OK){
      return -1;
    }
    
    HP_info hi;
    hi.file_desc = file_desc;
    hi.numofrecs_allowed = (BF_BLOCK_SIZE - sizeof(HP_block_info))/sizeof(Record);
    hi.hp_or_ht_or_sht = 0;

    HP_block_info hbi;
    hbi.number_of_records = 0;
    hbi.next = NULL;
    
    char* dat = BF_Block_GetData(block);
    memcpy(dat,&hi,sizeof(HP_info)*sizeof(char));
    memcpy(dat+sizeof(HP_info),&hbi,sizeof(HP_block_info)*sizeof(char));

    BF_Block_SetDirty(block);

    if (BF_UnpinBlock(block)){
      return -1;
    }
  
    if (BF_CloseFile(file_desc) != BF_OK){
      return -1;
    }

    return 0;
}

HP_info* HP_OpenFile(char *fileName){
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

  HP_info* tochange = for_return;
  tochange->file_desc = file_desc;  // Updating the file_desc of the first block with the new file_desc

  BF_Block_SetDirty(curr_block);
  
  HP_info* info = malloc(sizeof(HP_info));  
  memcpy(info, for_return, sizeof(HP_info));  // Creating a copy of file's HP_info to return

  if (info->hp_or_ht_or_sht != 0){
    free(info);
    return NULL;
  }

  if (BF_UnpinBlock(curr_block) != BF_OK){
    return NULL;
  }
  
  return info;
}


int HP_CloseFile( HP_info* hp_info ){
  int* last = malloc(sizeof(int));
  if(BF_GetBlockCounter(hp_info->file_desc, last) != BF_OK){
    return -1;
  }

  BF_Block* block;

  BF_Block_Init(&block);

  BF_GetBlock(hp_info->file_desc, 0, block);

  void* blockdata = BF_Block_GetData(block);

  BF_Block_Destroy(&block);

  for (int i = 1 ; i < *last ; i++){
    BF_Block* block;

    BF_Block_Init(&block);

    BF_GetBlock(hp_info->file_desc, i, block);

    blockdata = BF_Block_GetData(block);

    BF_Block_Destroy(&block);
  }

  if (BF_CloseFile(hp_info->file_desc) != BF_OK){
      return -1;
  }
  
  free(last);
  free(hp_info);
  return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){

  int* last = malloc(sizeof(int));
  BF_ErrorCode error = BF_GetBlockCounter(hp_info->file_desc, last);
  if (error != BF_OK){
    return error;
  }
  
  BF_Block* block;

  BF_Block_Init(&block);

  error = BF_GetBlock(hp_info->file_desc, *last - 1, block);
  if (error != BF_OK){
    return error;
  }
  void* blockdata = BF_Block_GetData(block);
  
  HP_block_info* hbi = malloc(sizeof(HP_block_info));
  if (*last - 1 > 0){   // If there's more than one block (the first block which only shows information about the file)
    memcpy(hbi,blockdata + sizeof(Record)*hp_info->numofrecs_allowed,sizeof(HP_block_info));
  }
  else{                 // If there's just one block
    memcpy(hbi,blockdata + sizeof(HP_info),sizeof(HP_block_info));
  }
  
  if (*last - 1 > 0 && hbi->number_of_records < hp_info->numofrecs_allowed){  // More than one block and the last one not full

    memcpy(blockdata + sizeof(Record)*hbi->number_of_records++, &record, sizeof(Record));

    memcpy(blockdata + sizeof(Record)*hp_info->numofrecs_allowed,hbi,sizeof(HP_block_info));

    BF_Block_SetDirty(block);

    error = BF_UnpinBlock(block);
    if (error != BF_OK){
      return error;
    }

    int last2 = *last;
    free(last);
    free(hbi);
    return last2 - 1;
  }
  
  // Only one block in the file or the last one is full
  BF_Block* new_block;
  
  BF_Block_Init(&new_block);

  // Allocating a new block in which we will save the record in
  error = BF_AllocateBlock(hp_info->file_desc, new_block);
  if (error != BF_OK){
    return error;
  }

  HP_block_info new_block_info;
  new_block_info.number_of_records = 1;
  new_block_info.next = NULL;
  
  void* dat = BF_Block_GetData(new_block);
  memcpy(dat, &record, sizeof(Record));
  memcpy(dat + hp_info->numofrecs_allowed*sizeof(Record), &new_block_info, sizeof(HP_block_info));

  BF_Block_SetDirty(new_block);

  hbi->next = new_block;    // Updating the previous block's next pointer

  if (*last - 1 > 0){       // If there's more than one block (the first block which only shows information about the file)
    memcpy(blockdata + sizeof(Record)*hp_info->numofrecs_allowed,hbi,sizeof(HP_block_info));
  }
  else{                     // If there's only one block in the file
    memcpy(blockdata + sizeof(HP_info),hbi,sizeof(HP_block_info));
  }

  BF_Block_SetDirty(block);

  error = BF_UnpinBlock(block);
  if (error != BF_OK){
    return error;
  }

  error = BF_UnpinBlock(new_block);
  if (error != BF_OK){
    return error;
  }

  int last2 = *last;
  free(last);
  free(hbi);
  return last2;
}

int HP_GetAllEntries(HP_info* hp_info, int value){
    int* last = malloc(sizeof(int));
    if (BF_GetBlockCounter(hp_info->file_desc, last) != BF_OK){
        return -1;
    }

    int counter = 0;
    for (int i = 1 ; i < *last ; i++){
        BF_Block* block;

        BF_Block_Init(&block);

        if (BF_GetBlock(hp_info->file_desc, i, block) != BF_OK){
            return -1;
        }
        void* blockdata = BF_Block_GetData(block);

        HP_block_info* hbi = malloc(sizeof(HP_block_info));
        memcpy(hbi,blockdata + sizeof(Record)*hp_info->numofrecs_allowed, sizeof(HP_block_info));
        
        // Checking every record of the current block
        for (int i = 0 ; i < hbi->number_of_records ; i++){
            Record* rec = malloc(sizeof(Record));
            memcpy(rec, blockdata + i*sizeof(Record), sizeof(Record)*sizeof(char));

            if (rec->id != value){
              free(rec);
              continue;
            }

            printRecord(*rec);
            free(rec);
        }

        if (BF_UnpinBlock(block) != BF_OK){
            return -1;
        }

        free(hbi);
        counter++;
    }

    free(last);
    return counter;
}

