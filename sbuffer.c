
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "lib/dplist.h"
#include <time.h>
#include "config.h"
#include "sbuffer.h"
#define TRUE 1
#define FALSE 0

#define SBUFFER_FAILURE -1
#define SBUFFER_SUCCESS 0
#define SBUFFER_NO_DATA 1


typedef int status;



sensor_data_t *copy_sensor_data(sensor_data_t *old_sensor) {
    sensor_data_t *new_sensor = (sensor_data_t*)malloc(sizeof(sensor_data_t));
    new_sensor->id=old_sensor->id;
    new_sensor->ts = old_sensor->ts;
    new_sensor->value=old_sensor->value;
    new_sensor->read_by_storage_mgr=old_sensor->read_by_storage_mgr;
    new_sensor->read_by_data_mgr=old_sensor->read_by_data_mgr;
    
    return new_sensor;
}

void free_sensor_data(sensor_data_t **sensor_to_free) {
    free(*sensor_to_free);
    *sensor_to_free = NULL;
    return;
}

int compare_sensor_data(sensor_data_t *sensor1, sensor_data_t *sensor2) {
    if(sensor1->id > sensor2->id) return 1;
    if(sensor1->id < sensor2->id) return -1;
    return 0;
}



int sbuffer_init(dplist_t ** buffer)
{



    *buffer =dpl_create(  (void *(*)(void *))copy_sensor_data,
                        (void (*)(void **))free_sensor_data,
                        (int (*)(void *, void *))compare_sensor_data
                    );
return SBUFFER_SUCCESS;

}


int sbuffer_free(dplist_t ** buffer){

dpl_free(buffer,true);
return SBUFFER_SUCCESS;
}


int sbuffer_insert(dplist_t * buffer, sensor_data_t * data){



dpl_insert_at_index(buffer,data,0,false);

return SBUFFER_SUCCESS;

}


int modify_buffer(dplist_t* buffer, sensor_data_t * data,int statusReg,int newValue){


sensor_data_t * dataToModify;


dataToModify=dpl_get_element_at_index(buffer,dpl_get_index_of_element(buffer,data));

if(statusReg==READ_BY_DATA_MGR){
dataToModify->read_by_data_mgr=newValue;
}
else if(statusReg==READ_BY_STORAGE_MGR)
{
dataToModify->read_by_storage_mgr=newValue;
}

return SBUFFER_SUCCESS;

}






int sbuffer_remove(dplist_t * buffer,sensor_data_t * data){


dpl_remove_element(buffer,data,true);
return SBUFFER_SUCCESS;



}








