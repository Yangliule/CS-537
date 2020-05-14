#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <semaphore.h>
#include "mapreduce.h"

Mapper mapper;
Reducer reducer;
Combiner combiner;
Partitioner partitioner;
CombineGetter cGetter;
ReduceGetter rGetter;

pthread_mutex_t file_lock;
int cur_file = 0; 
int num_files = -1; 
int num_reducer; 

//The idea of this data structure and the sorting logic is consulted with Yuxuan Liu and wiki page
typedef struct Reducer_Node {
    char *key;
    char *val;
    pthread_mutex_t key_lock;
}Reducer_Node;

typedef struct Reducer_List {
    Reducer_Node *store;
    pthread_mutex_t reducer_list_lock;
    int cur_size;
    int used;
    Reducer_Node *getkey;
    Reducer_Node *lastkey;
}Reducer_List;

pthread_mutex_t data_lock;

typedef struct{
    char*** arr;
    int num_key;
    int* num_val;
}list;

typedef struct {
    pthread_t p;
    int dirty;
}thread_info;

char*** get_combine_p;
int* num_get_combine;
int map_cnt;
list* combine_list_arr;
list l;
thread_info* info;
// typedef struct Combiner_Val_Node{
//     char* val;
//     struct Combiner_Val_Node* next;
// } Combiner_Val_Node;

// typedef struct Combiner_Key_Node{
//     char* key;
//     Combiner_Val_Node* head;
//     int val_list_length;
//     struct Combiner_Key_Node* next;
//     pthread_mutex_t combiner_Node_lock;
// } Combiner_Key_Node;

// typedef struct Combiner_List{
//     Combiner_Key_Node* head;
//     int num_keys;
//     pthread_mutex_t combiner_List_lock;
// } Combiner_List;

Reducer_List* storage;
//Combiner_List* combiner_storage;

//void MR_EmitToCombiner(char *passed_key, char *passed_value){

    // if(combiner_storage ->num_keys == 0){
  
    //     combiner_storage -> head -> key = strdup(passed_key);
   
    //     combiner_storage -> head -> head = malloc(sizeof(Combiner_Val_Node));
    //     combiner_storage ->head ->head ->val = strdup(passed_value);
    //     combiner_storage ->head ->head ->next = NULL;
    
    //     combiner_storage ->head->val_list_length = 1;
    //     combiner_storage ->num_keys = 1;
    //     combiner_storage -> head -> next = NULL;
        
    // }
    // else{
    //     Combiner_Key_Node* cur_node = combiner_storage -> head;

    //     while(cur_node != NULL){

    //         char *cur_key = cur_node ->key;
    //         if(strcmp(cur_key, passed_key) == 0){
     
    //             Combiner_Val_Node* cur_val_node = cur_node->head;
    //             while(cur_val_node != NULL){
    //                 cur_val_node = cur_val_node ->next;
    //             }
    //             cur_val_node = malloc(sizeof(Combiner_Val_Node));
    //             cur_val_node ->val = strdup(passed_value);
    //             cur_val_node ->next = NULL;
    //             cur_node ->val_list_length ++;
    //             return;
    //         }

    //         cur_node = cur_node -> next;

    //     }

    //     combiner_storage -> num_keys ++;
   
    //     cur_node = malloc(sizeof(Combiner_Key_Node));
  
    //     cur_node -> head = malloc(sizeof(Combiner_Val_Node));

    //     cur_node -> key = strdup(passed_key);
    //     cur_node -> next = NULL;
    //     cur_node ->head->val = strdup(passed_value);
    //     cur_node ->head->next = NULL;
    // }

//}
int get_t(){
    for(int i =0; i<map_cnt; i++){
        if(info[i].p == pthread_self()){
            return i;
        }
    }
    return -1;
}

list* get_list(){
    return &combine_list_arr[get_t()];
}

void MR_EmitToCombiner(char* key, char* value){
    pthread_mutex_lock(&data_lock);
    list* cur_list = get_list();
    for(int i =0; i<cur_list->num_key; i++){//search for if mapped
        if(strcmp(cur_list->arr[i][0], key) == 0){//found key mapped
            cur_list->num_val[i]++;
            cur_list->arr[i] = realloc(cur_list->arr[i], sizeof(char*)*(cur_list->num_val[i]+1));
            cur_list->arr[i][cur_list->num_val[i]] = strdup(value);
            pthread_mutex_unlock(&data_lock);
            return;
        }
    }
    //new key because not found
    cur_list->num_key ++;
    cur_list->arr = realloc(cur_list->arr, sizeof(char**)*cur_list->num_key);
    cur_list->arr[cur_list->num_key-1] = malloc(2*sizeof(char*));
    cur_list->arr[cur_list->num_key-1][0] = strdup(key);
    cur_list->arr[cur_list->num_key-1][1] = strdup(value);
    cur_list->num_val = realloc(cur_list->num_val, sizeof(int)*cur_list->num_key);
    cur_list->num_val[cur_list->num_key-1] = 1;
    pthread_mutex_unlock(&data_lock);
    return;
}

char* combiner_get_next(char *passed_key){
    pthread_mutex_lock(&data_lock);
    list* cur_list = get_list();
    char* res = NULL;
    for(int i =0; i<cur_list->num_key; i++){
        if(strcmp(cur_list->arr[i][0], passed_key) == 0){//find the key
            if(cur_list->num_val[i]==0){//no value for this key
                goto out;
            }
            int last_index = cur_list->num_val[i];
            res = strdup(cur_list->arr[i][last_index]);//need to return the first value
            cur_list->num_val[i]--;//decrement num_value
            cur_list->arr[i][last_index] = NULL;
        }
    }
    out:
    pthread_mutex_unlock(&data_lock);
    return res;
    // Combiner_Key_Node* cur_node = combiner_storage -> head;
    


    // while(cur_node != NULL){
        
    //     if(strcmp(cur_node -> key, passed_key) == 0){
    //         if(cur_node ->val_list_length <= 0){
    //             return NULL;
    //         }
    //         else{
     
    //             if(cur_node ->head == NULL){
           
    //                 return NULL;
    //             }
     
    //             cur_node ->val_list_length --;
           
    //             char* res = cur_node ->head ->val;
     
    //             cur_node ->head = cur_node->head->next;
        
    //             return res;
    //         }
    //     }
    //     cur_node = cur_node -> next;
    // }
    // return NULL;
}

void MR_EmitToReducer(char *key, char *value){

    int partition = partitioner(key, num_reducer);
    Reducer_List* store = storage + partition;
    pthread_mutex_lock(&(store->reducer_list_lock));
    Reducer_Node* keyarray = store->store;

    if (store->cur_size == store->used) {
        keyarray = (Reducer_Node*)realloc(keyarray, 2 * store->cur_size * sizeof(Reducer_Node));
	    store->cur_size *= 2;
        memset((keyarray + store->used), 0,  (store->cur_size - store->used)*sizeof(Reducer_Node));
        store -> store = keyarray;
    }

    Reducer_Node newKey;
    newKey.key = calloc(1, strlen(key));
    newKey.val = calloc(1, strlen(value));
    strcpy(newKey.key, key);
    strcpy(newKey.val, value);
    pthread_mutex_init( &(newKey.key_lock), NULL);
    memcpy((store->store + store->used), &newKey, sizeof(Reducer_Node));
    store->used++;
    pthread_mutex_unlock( &(store->reducer_list_lock));
}

char* reducer_get_next(char *key, int partition_number){

    Reducer_List* store = storage + partition_number;
    Reducer_Node* curkey = store -> getkey;
    pthread_mutex_lock(&(curkey->key_lock));

    if(curkey->key == NULL || strcmp(store->lastkey->key, curkey->key) != 0){
        pthread_mutex_unlock(&(curkey->key_lock));
        return NULL;
    }
    char* val = curkey -> val;
    store->getkey = (Reducer_Node*)((void*)store->getkey + sizeof(Reducer_Node));
    pthread_mutex_unlock(&(curkey->key_lock));
    return val;
}


void free_list() {
    for (int i = 0; i < num_reducer; i++) {
        Reducer_List *store = storage + i;
        Reducer_Node *keyarray = store->store;
	for(int j = 0; j < store->cur_size; j++){
        Reducer_Node* keyNode = keyarray + j;
	    free(keyNode->key);
	    free(keyNode->val);
	    pthread_mutex_destroy(&(keyNode->key_lock));
	}
	pthread_mutex_destroy(&(store->reducer_list_lock));
	free(store->store);
	free(store->lastkey);
    }
    pthread_mutex_destroy(&file_lock);
    free(storage);
    storage = NULL;
}

int Node_cpr(const void*node1, const void*node2){
    char *key1 = ((Reducer_Node*)node1) -> key;
    char *key2 = ((Reducer_Node*)node2) -> key;
    char *val1 = ((Reducer_Node*)node1) -> val;
    char *val2 = ((Reducer_Node*)node2) -> val;
    int i = strcmp(key1, key2);
    if(i == 0) i = strcmp(val1, val2);
    return i;
}

//The idea of this function comes from yuxuan Liu and wiki page
void sort_list(){
    for(int i = 0; i < num_reducer; i++){
        Reducer_List* store = (storage + i);
        qsort(store->store, store->used, sizeof(Reducer_Node), Node_cpr);
    }
}


unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}



void *mapper_wrapper(void *argv){
    
    while(1) {
        pthread_mutex_lock(&file_lock);
        if (cur_file >= num_files) {
            pthread_mutex_unlock(&file_lock);
            return NULL;
        }
        char *curfile = *((char**)argv + (cur_file++));
        pthread_mutex_unlock(&file_lock);
        (*mapper)(curfile);
        if(combiner != NULL){
            list* cur_list = get_list();
            for(int j =0; j<cur_list->num_key; j++){
                (*combiner)(cur_list->arr[j][0], cGetter);
            }
        }
    }
}


void *reducer_wrapper(void* argv){
    int partition = *((int*)argv);
    Reducer_List* store = storage + partition;
    store -> getkey = store->store;
    char* newkey;
    Reducer_Node* end = store->store + store->used;
    
    store->lastkey = malloc(sizeof(Reducer_Node));
    store->lastkey->key = NULL;
    while(store->getkey < end){
        newkey = store->getkey->key;
        if(store->lastkey->key == NULL || strcmp(store->lastkey->key, newkey) != 0){
            store->lastkey->key = newkey;
            (*reducer)(newkey, NULL, reducer_get_next, partition);
        }
    }
   
    return NULL;
}



void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition){
    
    pthread_mutex_init(&file_lock, NULL);
    
    mapper = map;
    reducer = reduce;
    num_reducer = num_reducers;
    num_files = argc - 1;
    mapper = map;
    combiner = combine;
    cGetter = (CombineGetter) combiner_get_next;
    rGetter = (ReduceGetter) reducer_get_next;
    if (partition == NULL) partitioner = MR_DefaultHashPartition;
    else partitioner = partition;
    combine_list_arr = malloc(sizeof(l)*num_mappers);
    get_combine_p = malloc(sizeof(char**)*num_mappers);
    num_get_combine = malloc(sizeof(int)*num_mappers);
    //combiner_storage = malloc(sizeof(Combiner_List));
    storage = calloc(num_reducer, sizeof(Reducer_List));
    // combiner_storage ->num_keys = 0;
    // combiner_storage ->head = malloc(sizeof(Combiner_Key_Node));

    for(int i =0; i<num_mappers; i++){
        combine_list_arr[i].arr = NULL;
        combine_list_arr[i].num_key = 0;
        combine_list_arr[i].num_val = NULL;
        get_combine_p[i] = NULL;
        num_get_combine[i] = 0;
    }

    for(int i = 0; i < num_reducer; i++){
        Reducer_List* store = storage + i;
        store -> store = calloc(10, sizeof(Reducer_Node));
        store -> cur_size = 10;
        pthread_mutex_init(&(store->reducer_list_lock), NULL);
    }


    int num_threads =  (num_mappers < num_files) ? num_mappers : num_files;
    pthread_t threads[num_threads];
    cur_file = 0;

    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&threads[i], NULL, mapper_wrapper, argv + 1) != 0) {
            exit(1);
        }    
    }

    for (int i = 0; i < num_threads; i++) {

        pthread_join(threads[i], NULL);
    }

    sort_list();

    pthread_t rthreads[num_reducer];
    int* rargv[num_reducer];
    for (int i = 0; i < num_reducer; i++) {
	    rargv[i] = malloc(sizeof(int));
        *(rargv[i]) = i;
        pthread_create(&rthreads[i], NULL, reducer_wrapper, rargv[i]);
    }
    for (int i = 0; i < num_reducer; i++) {
        pthread_join(rthreads[i], NULL);
    }
    for(int i = 0; i < num_reducer; i++){
	free(rargv[i]); rargv[i] = NULL;
    }
    free_list();
}