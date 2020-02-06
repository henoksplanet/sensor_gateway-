#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <stdint.h>
#include <time.h>

typedef uint16_t sensor_id_t;
typedef  int status;
typedef int fd_number;
typedef uint16_t room_id_t;
typedef	double running_avg;
typedef double sensor_value_t;  
typedef time_t last_modified;   
typedef time_t sensor_ts_t;         // UTC timestamp as returned by time() - notice that the size of time_t is different on 32/64 bit machine

typedef struct {		//blueprint for the sensor data
  sensor_id_t id;
  sensor_value_t value;
  sensor_ts_t ts;
  status read_by_data_mgr;
  status read_by_storage_mgr; 
} sensor_data_t;

typedef struct {		//blueprint for the sensor node
  sensor_id_t id;
  room_id_t   room_id;
  running_avg running_avg_t;
  last_modified	last_modified_t; 
  double lastFive[5];
} sensor_node;



typedef struct sensor{
    sensor_id_t sensor_id;
    fd_number   fd_num;
    time_t  last_update;
}sensor;


struct logEvent{
time_t timestamp;
int logCode;
int logData;
double extras;
}typedef logEvent;


#endif /* _CONFIG_H_ */

