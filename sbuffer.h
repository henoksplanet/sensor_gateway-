#ifndef _SBUFFER_H_
#define _SBUFFER_H_

#include "config.h"
#include "lib/dplist.h"
#define SBUFFER_FAILURE -1
#define SBUFFER_SUCCESS 0
#define SBUFFER_NO_DATA 1

#define READ_BY_DATA_MGR 0
#define READ_BY_STORAGE_MGR 1


typedef struct sbuffer sbuffer_t;





int sbuffer_init(dplist_t ** buffer);


/*
 * All allocated resources are freed and cleaned up
 * Returns SBUFFER_SUCCESS on success and SBUFFER_FAILURE if an error occured
 */
int sbuffer_free(dplist_t ** buffer);


/*
 * Removes the first sensor data in 'buffer' (at the 'head') and returns this sensor data as '*data'  
 * 'data' must point to allocated memory because this functions doesn't allocated memory
 * If 'buffer' is empty, the function doesn't block until new sensor data becomes available but returns SBUFFER_NO_DATA
 * Returns SBUFFER_SUCCESS on success and SBUFFER_FAILURE if an error occured
 */
int sbuffer_remove(dplist_t * buffer,sensor_data_t * data);


/* Inserts the sensor data in 'data' at the end of 'buffer' (at the 'tail')
 * Returns SBUFFER_SUCCESS on success and SBUFFER_FAILURE if an error occured
*/
int sbuffer_insert(dplist_t * buffer, sensor_data_t * data);


#endif  //_SBUFFER_H_

