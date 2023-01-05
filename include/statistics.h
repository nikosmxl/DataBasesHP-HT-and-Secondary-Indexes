#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"
#include "hp_file.h"

int HashStatistics(char*);

int HashStatistics_HP(char*, HP_info*);

int HashStatistics_HT(char*, HT_info*);

int HashStatistics_SHT(char*, SHT_info*);