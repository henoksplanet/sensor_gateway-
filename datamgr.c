#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "lib/dplist.h"
#include "config.h"
#include<time.h>
#include "datamgr.h"
#include <unistd.h>


int filesize=0;

int extraSensors[100]={0};

int extraSensorCounter=0;

int fileSize(FILE *fPointer);
double averageCalculator(double array[],int size);
int checkArray(double array[],int size);
void displayArray(double array[],int size);
void shiftArray(double array[],int size,double newdata);
void displayAll();

logEvent log1;


sensor_node *copy_sensor_node(sensor_node *sensor_node_t) {

sensor_node *new_sensor_node = (sensor_node*)malloc(sizeof(sensor_node));
new_sensor_node->id =sensor_node_t->id;
new_sensor_node->last_modified_t=sensor_node_t->last_modified_t;
new_sensor_node->room_id = sensor_node_t->room_id;

return new_sensor_node;
}

void free_sensor_node(sensor_node **sensor_node_t) {

free(*sensor_node_t);
*sensor_node_t = NULL;

return;
}

int compare_sensor_node(sensor_node *sensor_value_t_1, sensor_node *sensor_value_t_2) {
   if((int)sensor_value_t_1->id > (int)sensor_value_t_2->id) return 1;
    if((int)sensor_value_t_1->id < (int)sensor_value_t_2->id) return -1;
    return 0;
}



dplist_t * datamgr_parse_room_sensor(FILE * fp_sensor_map){
 char room_id_t[3];
 char sensor_id_t[3];
 dplist_t * list;
 
list = dpl_create(  (void *(*)(void *))copy_sensor_node,
                    (void (*)(void **))free_sensor_node,
                    (int (*)(void *, void *))compare_sensor_node
                    );

int counter=-1;
  
FILE *fp = fp_sensor_map;
      if(fp == NULL) {
          perror("Unable to open file!");
         exit(1);
      }
     char * pch;
     
     char chunk[128];

     int i=0;

     while(fgets(chunk, sizeof(chunk), fp) != NULL) {
     
      pch = strtok (chunk, " ");           

  while (pch != NULL)
  {
    if (i%2==0){
    strcpy(room_id_t, pch); 
    }  ;
    if (i%2!=0){
    strcpy(sensor_id_t, pch);
    counter++;
     sensor_node *room = (sensor_node*) malloc(sizeof(sensor_node));
    room->room_id=atoi(room_id_t);
    room->id =atoi(sensor_id_t);
    room->running_avg_t=0;
    room->last_modified_t=time(NULL);


for(int i=0;i<RUN_AVG_LENGTH;i++)
{
    room->lastFive[i]=0;
    }


   list=dpl_insert_at_index(list, room, counter, false); 
    }  ;
    pch = strtok (NULL, " "); 
    i++;
  }  
    }
     fclose(fp);
return list;
}




dplist_t * datamgr_parse_sensor_data(FILE * fPointer){

 dplist_t * list2;
 
 list2 = dpl_create(  (void *(*)(void *))copy_sensor_node,
                    (void (*)(void **))free_sensor_node,
                    (int (*)(void *, void *))compare_sensor_node
                    );

 
 FILE *file =fPointer;

   int counter=0;
   for(int i=0;i<filesize;i=i+18){
       sensor_data_t *sensorData= (sensor_data_t*) malloc(sizeof(sensor_data_t)); 

       fseek ( file , i, SEEK_SET );
       fread(&(sensorData->id), sizeof(uint16_t), 1, file);
       fseek ( file , i+2, SEEK_SET );
       fread(&(sensorData->value), sizeof(double), 1, file);
       fseek ( file , i+10, SEEK_SET );
       fread(&(sensorData->ts), sizeof(time_t), 1, file);

       dpl_insert_at_index(list2, sensorData, counter, false); 
       counter++;
       };
 
    printf ("counter :%i\n",counter);
    fclose(file);

return list2;
}

int fileSize(FILE * fPointer){
  
    FILE *fp=fPointer;
    
    int size = 0;
   
    if (fp == NULL)
        printf("\nFile unable to open ");
    else 
    fseek(fp, 0, 2);    /* file pointer at the end of file */
    size = ftell(fp);   /* take a position of file pointer un size variable */
       
    fclose(fp);

    return size;
}


dplist_t * list;

dplist_t * list_data;


void datamgr_parse_sensor_files(FILE * fp_sensor_map, FILE * fp_sensor_data)

{

FILE * fPointerSize=fopen("sensor_data","rb");
filesize=fileSize(fPointerSize);

list=datamgr_parse_room_sensor(fp_sensor_map);
list_data=datamgr_parse_sensor_data(fp_sensor_data);

int index=0;
for(int i=0;i<dpl_size(list_data);i++){

if(dpl_get_index_of_element(list,dpl_get_element_at_index(list_data,i))==-1){ //unknown sensor 

    sensor_data_t *sensorData_t=dpl_get_element_at_index(list_data,i);

    fprintf(stderr, "new sensor ID found with id %i", sensorData_t->id);

}


  if(dpl_get_index_of_element(list,dpl_get_element_at_index(list_data,i))!=-1){
    
index=dpl_get_index_of_element(list,dpl_get_element_at_index(list_data,i));

sensor_node * sensor_node_t=dpl_get_element_at_index(list,index);

sensor_data_t * sensor_data=dpl_get_element_at_index(list_data,i);

shiftArray(sensor_node_t->lastFive,RUN_AVG_LENGTH,sensor_data->value);

sensor_node_t->running_avg_t=averageCalculator(sensor_node_t->lastFive,RUN_AVG_LENGTH);

if((float)averageCalculator(sensor_node_t->lastFive,RUN_AVG_LENGTH)!=0){

if((float)averageCalculator(sensor_node_t->lastFive,RUN_AVG_LENGTH)<=SET_MIN_TEMP)
{
  fprintf(stderr, "room-id %i is too cold at %lu\n", sensor_node_t->room_id,sensor_data->ts);
}
if((float)averageCalculator(sensor_node_t->lastFive,RUN_AVG_LENGTH)>=SET_MAX_TEMP)
{
  fprintf(stderr, "room-id %i is too hot  at %lu\n", sensor_node_t->room_id,sensor_data->ts);
}


}


sensor_node_t->last_modified_t=sensor_data->ts; //updates time stamp

  }
}

}




double averageCalculator(double array[],int size){

if(checkArray(array,size)!=-1){
  return 0;
}

double sum=0;
for(int i=0;i<size;i++){
sum=sum+array[i];
}
double average=sum/size;
return average;
}



int checkArray(double array[],int size){

for(int i=0;i<size;i++){
  if(array[i]==0){   //if the shit is not full yet 
    return i;
  }
}
return -1;
}



void shiftArray(double array[],int size,double newdata){

int n=0;
for(int i=0;i<size-1;i++){
array[n]=array[n+1];
n++;
}
array[size-1]=newdata;

}

void displayAll(){

for(int i=0;i<dpl_size(list);i++){

sensor_node *sensorNode=dpl_get_element_at_index(list,i);

printf("index %i with sensor-ID %i has running average of %f and last time stamp was %lu\n",i,sensorNode->id,sensorNode->running_avg_t,sensorNode->last_modified_t);
displayArray(sensorNode->lastFive,RUN_AVG_LENGTH);

}

}




void displayArray(double array[],int size){
for(int i=0;i<size;i++){
printf("%f,",*(array + i));
}
printf("\n\n");
}


void datamgr_free(){
dpl_free(&list,true);
dpl_free(&list_data,true);

}



uint16_t datamgr_get_room_id(sensor_id_t sensor_id){
int index=0;
int room_id=0;
sensor_data_t *sensorData=(sensor_data_t *)malloc(sizeof(sensor_data_t));

sensorData->id=sensor_id;
sensorData->ts=0;
sensorData->value=0;


if(dpl_get_index_of_element(list,sensorData)==-1 || sensor_id<0){
free(sensorData);
return 0;
}

index=dpl_get_index_of_element(list,sensorData);
sensor_node *sensorNode=dpl_get_element_at_index(list,index);

room_id=sensorNode->room_id;
free(sensorData);
return room_id;

}


sensor_value_t datamgr_get_avg(sensor_id_t sensor_id){

    int index=0;
    double average=0;
    sensor_data_t *sensorData=(sensor_data_t *)malloc(sizeof(sensor_data_t));

    sensorData->id=sensor_id;
    sensorData->ts=0;
    sensorData->value=0;


    if(dpl_get_index_of_element(list,sensorData)==-1 || sensor_id<0){
      free(sensorData);
      return 0;
      }

    index=dpl_get_index_of_element(list,sensorData);
    sensor_node *sensorNode=dpl_get_element_at_index(list,index);

    average=sensorNode->running_avg_t;
    free(sensorData);
return average;

}


time_t datamgr_get_last_modified(sensor_id_t sensor_id){

int index=0;
time_t last_modified_t=0;
sensor_data_t *sensorData=(sensor_data_t *)malloc(sizeof(sensor_data_t));

sensorData->id=sensor_id;
sensorData->ts=0;
sensorData->value=0;


if(dpl_get_index_of_element(list,sensorData)==-1 || sensor_id<0){
free(sensorData);
return 0;
}

index=dpl_get_index_of_element(list,sensorData);
sensor_node *sensorNode=dpl_get_element_at_index(list,index);

last_modified_t=sensorNode->last_modified_t;
free(sensorData);
return last_modified_t;

}



int datamgr_get_total_sensors(){


return dpl_size(list);

}



void writer(int fd[],long int timestamp,int logCode,int logData,double extras){
         
        log1.timestamp=timestamp;
        log1.logCode=logCode;
        log1.logData=logData; 
        log1.extras=extras;
         write(fd[1],&log1,sizeof(logEvent));

}