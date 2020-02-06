#ifndef __CONMGR_H__
#define __CONMGR_H__

#include <stdio.h>
#include <stdlib.h>
#include <poll.h>
#include <assert.h>
#include <inttypes.h>

#include "lib/tcpsock.h"
#include "lib/dplist.h"
#include "config.h"
#include "sbuffer.h"
#include <pthread.h>

#include "sensor_db.h"

//#ifndef TIMEOUT
 // #error TIMEOUT not specified!(in seconds)
//#endif


typedef struct pollfd polldescr;

typedef struct{
  polldescr file_descriptors; //dit is eigenlijk een array
  time_t last_record;
  sensor_id_t sensor_id;
  tcpsock_t* socket_id;
} pollinfo;

/*This method holds the core functionality of your connmgr. It starts listening on the given port and
when when a sensor node connects it writes the data to a sensor_data_recv file. This file must have the
 same format as the sensor_data file in assignment 6 and 7.*/

void connmgr_listen(int port_number,dplist_t ** buffer,pthread_mutex_t *lock,dplist_t * room_sensor_map_main,pthread_mutex_t * lock_log,int logFIFO[]);

/*This method should be called to clean up the connmgr, and to free all used memory. After this no new connections will be accepted*/
void connmgr_free();

#endif  //__CONMGR_H__