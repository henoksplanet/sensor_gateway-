

      /***********************************************************************************/
     /* Part of the implementation in this code is from this ibm link                  */
   /* https://www.ibm.com/support/knowledgecenter/en/ssw_ibm_i_71/rzab6/poll.htm     */
  /*********************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include "config.h"
#include "lib/dplist.h"
#include <pthread.h>
#include<string.h>

#include "connmgr.h"
#include "datamgr.h"
#include "sensor_db.h"

#define TRUE             1
#define FALSE            0

#if !defined(DEBUG_MODE)
    #define DEBUG_MODE 0
#endif

#if !defined(TIMEOUT)
    #define TIMEOUT 6
#endif


#define MAX_NUMBER_OF_CLIENTS 5000

typedef int fd_number;



sensor *copy_sensor(sensor *old_sensor) {
    sensor *new_sensor = (sensor*)malloc(sizeof(sensor));
    new_sensor->sensor_id=old_sensor->sensor_id;
    new_sensor->fd_num = old_sensor->fd_num;
    new_sensor->last_update = old_sensor->last_update;
    
    return new_sensor;
}

void free_sensor(sensor **sensor_to_free) {
    free(*sensor_to_free);
    *sensor_to_free = NULL;
    return;
}

int compare_sensor(sensor *sensor1, sensor *sensor2) {
    if(sensor1->fd_num > sensor2->fd_num) return 1;
    if(sensor1->fd_num < sensor2->fd_num) return -1;
    return 0;
}



dplist_t * room_sensor_map;  
dplist_t * list;
DBCONN * conn;




void connmgr_listen(int port_number,dplist_t ** buffer,pthread_mutex_t *lock,dplist_t * room_sensor_map_main,pthread_mutex_t * lock_log,int  logFIFO[])
{

      room_sensor_map=room_sensor_map_main;

 
      int SERVER_PORT = port_number; 

      printf("listening at port %i\n",SERVER_PORT);
    

      list = dpl_create(  (void *(*)(void *))copy_sensor,
                   (void (*)(void **))free_sensor,
                  (int (*)(void *, void *))compare_sensor
                    );


  int    rc, on = 1;
  int    listen_sd = -1, new_sd = -1;
  int    end_server = FALSE, compress_array = FALSE;
  int    close_conn;
  struct sockaddr_in6   addr;
  int    timeout;
  struct pollfd fds[MAX_NUMBER_OF_CLIENTS];
  int    nfds = 1, current_size = 0, i, j;

  /*************************************************************/
  /* Create an AF_INET6 stream socket to receive incoming      */
  /* connections on                                            */
  /*************************************************************/
  listen_sd = socket(AF_INET6, SOCK_STREAM, 0);
  if (listen_sd < 0)
  {
    perror("socket() failed");
    exit(-1);
  }

  /*************************************************************/
  /* Allow socket descriptor to be reuseable                   */
  /*************************************************************/
  rc = setsockopt(listen_sd, SOL_SOCKET,  SO_REUSEADDR,
                  (char *)&on, sizeof(on));
  if (rc < 0)
  {
    perror("setsockopt() failed");
    close(listen_sd);
    exit(-1);
  }

  /*************************************************************/
  /* Set socket to be nonblocking. All of the sockets for      */
  /* the incoming connections will also be nonblocking since   */
  /* they will inherit that state from the listening socket.   */
  /*************************************************************/
  rc = ioctl(listen_sd, FIONBIO, (char *)&on);
  if (rc < 0)
  {
    perror("ioctl() failed");
    close(listen_sd);
    exit(-1);
  }

  /*************************************************************/
  /* Bind the socket                                           */
  /*************************************************************/
  memset(&addr, 0, sizeof(addr));
  addr.sin6_family      = AF_INET6;
  memcpy(&addr.sin6_addr, &in6addr_any, sizeof(in6addr_any));
  addr.sin6_port        = htons(SERVER_PORT);
  rc = bind(listen_sd,
            (struct sockaddr *)&addr, sizeof(addr));
  if (rc < 0)
  {
    perror("bind() failed");
    close(listen_sd);
    exit(-1);
  }

  /*************************************************************/
  /* Set the listen back log                                   */
  /*************************************************************/
  rc = listen(listen_sd, 32);
  if (rc < 0)
  {
    perror("listen() failed");
    close(listen_sd);
    exit(-1);
  }

  /*************************************************************/
  /* Initialize the pollfd structure                           */
  /*************************************************************/
  memset(fds, 0 , sizeof(fds));

  /*************************************************************/
  /* Set up the initial listening socket                        */
  /*************************************************************/
  fds[0].fd = listen_sd;
  fds[0].events = POLLIN;
  /*************************************************************/
  /* Initialize the timeout to 3 minutes. If no                */
  /* activity after 3 minutes this program will end.           */
  /* timeout value is based on milliseconds.                   */
  /*************************************************************/
  timeout = (TIMEOUT* 1000);

  /*************************************************************/
  /* Loop waiting for incoming connects or for incoming data   */
  /* on any of the connected sockets.                          */
  /*************************************************************/
 
  do{
    
  /***************************************************************************************/
  /* Loop through all the file descriptors and checks is there is any thats is timeout   */                        
  /*************************************************************/
 

              time_t now;
              for(int i=0;i<dpl_size(list);i++)
                {  
                    now=time(NULL);

                    if(((sensor* )(dpl_get_element_at_index(list,i)))->last_update<now-TIMEOUT){
 
                          printf("sensor_id %i has timeout\n",((sensor* )(dpl_get_element_at_index(list,i)))->sensor_id);
                          close(((sensor* )(dpl_get_element_at_index(list,i)))->fd_num);

                      }  

                 } 

    /***********************************************************/
    /* Call poll() and wait 3 minutes for it to complete.      */
    /***********************************************************/
    //printf("Waiting on poll()...\n");
    rc = poll(fds, nfds, timeout);

    /***********************************************************/
    /* Check to see if the poll call failed.                   */
    /***********************************************************/
    if (rc < 0)
    {
      perror("  poll() failed");
      break;
    }

    /***********************************************************/
    /* Check to see if the 3 minute time out expired.          */
    /***********************************************************/
    if (rc == 0)
    {
      printf("server timeout...  End program.\n");
      break;
    }


    /***********************************************************/
    /* One or more descriptors are readable.  Need to          */
    /* determine which ones they are.                          */
    /***********************************************************/
    current_size = nfds;
    for (i = 0; i < current_size; i++)
    {
      /*********************************************************/
      /* Loop through to find the descriptors that returned    */
      /* POLLIN and determine whether it's the listening       */
      /* or the active connection.                             */
      /*********************************************************/
      if(fds[i].revents == 0)
        continue;

      /*********************************************************/
      /* If revents is not POLLIN, it's an unexpected result,  */
      /* log and end the server.                               */
      /*********************************************************/
      if(fds[i].revents != POLLIN)
      {

          close(fds[i].fd);
         
         sensor *sensorPH = (sensor*) malloc(sizeof(sensor));
         sensorPH->fd_num=fds[i].fd;
         
          printf("a Sensor node with %i has closed the connection\n",((sensor* )(dpl_get_element_at_index(list,dpl_get_index_of_element(list,sensorPH))))->sensor_id);

          pthread_mutex_lock(lock_log);
                  time_t now=time(NULL);
                

                  writer(logFIFO,now,2,((sensor* )(dpl_get_element_at_index(list,dpl_get_index_of_element(list,sensorPH))))->sensor_id,0);
                  pthread_mutex_unlock(lock_log); 

          dpl_remove_at_index(list,dpl_get_index_of_element(list,sensorPH),true);  
          free(sensorPH);
          sensorPH=NULL;


          fds[i].fd = -1;
           for (i = 0; i < nfds; i++)
      {
        if (fds[i].fd == -1)
        {
          for(j = i; j < nfds; j++)
          {
            fds[j].fd = fds[j+1].fd;
          }
          i--;
          nfds--;
       
        }
      }
      continue;
      }





      if (fds[i].fd == listen_sd)
      {
        /*******************************************************/
        /* Listening descriptor is readable.                   */
        /*******************************************************/
        //printf("Listening socket is readable\n");

        /*******************************************************/
        /* Accept all incoming connections that are            */
        /* queued up on the listening socket before we         */
        /* loop back and call poll again.                      */
        /*******************************************************/
        do
        {
          /*****************************************************/
          /* Accept each incoming connection. If               */
          /* accept fails with EWOULDBLOCK, then we            */
          /* have accepted all of them. Any other              */
          /* failure on accept will cause us to end the        */
          /* server.                                           */
          /*****************************************************/
          new_sd = accept(listen_sd, NULL, NULL);
          if (new_sd < 0)
          {
            if (errno != EWOULDBLOCK)
            {
              perror("  accept() failed");
              end_server = TRUE;
            }
            break;
          }

          /*****************************************************/
          /* Add the new incoming connection to the            */
          /* pollfd structure                                  */
          /*****************************************************/

          fds[nfds].fd = new_sd;
          fds[nfds].events = POLLIN;
          nfds++;
          

          
          /*****************************************************/
          /* Loop back up and accept another incoming          */
          /* connection                                        */
          /*****************************************************/
        } while (new_sd != -1);
      }

      /*********************************************************/
      /* This is not the listening socket, therefore an        */
      /* existing connection must be readable                  */
      /*********************************************************/

      else
      {
     //   printf("  Descriptor %d is readable\n", fds[i].fd);
        close_conn = FALSE;
        /*******************************************************/
        /* Receive all incoming data on this socket            */
        /* before we loop back and call poll again.            */
        /*******************************************************/

       // do
        //{
          /*****************************************************/
          /* Receive data on this connection until the         */
          /* recv fails with EWOULDBLOCK. If any other         */
          /* failure occurs, we will close the                 */
          /* connection.                                       */
          /*****************************************************/
         
          
         sensor_id_t id;
	       sensor_value_t value;
	       sensor_ts_t ts;
         
          
          rc = read(fds[i].fd, &id, sizeof(sensor_id_t));
          read(fds[i].fd, &value, sizeof(sensor_value_t));
          read(fds[i].fd, &ts, sizeof(sensor_ts_t));
          
           
           sensor_data_t * sensor_reading=(sensor_data_t *) malloc(sizeof(sensor_data_t));

          sensor_reading->id=id;
        
         if(dpl_get_index_of_element(room_sensor_map,sensor_reading)!=-1){

               sensor_data_t * dataToBuffer=(sensor_data_t *)malloc(sizeof(sensor_data_t));
               dataToBuffer->id=id;
               dataToBuffer->ts=ts;
               dataToBuffer->value=value;
               dataToBuffer->read_by_data_mgr=0;
               dataToBuffer->read_by_storage_mgr=0;

        

               pthread_mutex_lock(lock);
          
               dpl_insert_at_index(*buffer,dataToBuffer,dpl_size(*buffer)+1,false);   //add the new data to the end of the buffer
          
               pthread_mutex_unlock(lock); 

         
              sensor *sensor1 = (sensor*) malloc(sizeof(sensor));
              sensor1->sensor_id=id;
              sensor1->fd_num=fds[i].fd;
              sensor1->last_update=ts;

              int index=dpl_get_index_of_element(list,sensor1);
         
              if(index==-1){
           
                  printf("a Sensor node with %i has opened a new connection\n",id);


                  pthread_mutex_lock(lock_log);
                  time_t now=time(NULL);
                

                  writer(logFIFO,now,1,id,0);
                  pthread_mutex_unlock(lock_log); 



                  list=dpl_insert_at_index(list, sensor1, i, true);
                  free(sensor1);
              }else 
              { 
                  ((sensor* )(dpl_get_element_at_index(list,dpl_get_index_of_element(list,sensor1))))->last_update=ts;   
                  free(sensor1);
              }
          
         
             free(sensor_reading);

            if(DEBUG_MODE==1)
             printf("sensor id:%i\tsensor-value:%f\ttimestamp:%lu\n",id,value,ts);
             
              }
         else
         {
             printf("unknown sensor-- blocked(sensor_node_id=%i)\n",id);
             pthread_mutex_lock(lock_log);
                                 
                  time_t now=time(NULL);
                  
             
                  writer(logFIFO,now,5,sensor_reading->id,0);

        pthread_mutex_unlock(lock_log);
       
             fds[i].fd=-1;
              free(sensor_reading);
       
         }
          
         

          if (rc < 0)
          {
            if (errno != EWOULDBLOCK)
            {
             if(DEBUG_MODE==1)
             {
              perror("  recv() failed---2");
             }
              close_conn = TRUE;
            }
            break;
          }

          /*****************************************************/
          /* Check to see if the connection has been           */
          /* closed by the client                              */
          /*****************************************************/
          if (rc == 0)
          {
           // printf("  Connection closed\n");
            close_conn = TRUE;
            break;
          }

          /*****************************************************/
          /* Data was received                                 */
          /*****************************************************/
         
          /*****************************************************/
          /* Echo the data back to the client                  */
          /*****************************************************/
          char data='h';
          rc = send(fds[i].fd, &data, sizeof(char), 0);
          if (rc < 0)
          {
           // perror("  send() failed...1");
            close_conn = TRUE;
            break;
          }  

      //  } while(TRUE);

        /*******************************************************/
        /* If the close_conn flag was turned on, we need       */
        /* to clean up this active connection. This            */
        /* clean up process includes removing the              */
        /* descriptor.                                         */
        /*******************************************************/
        if (close_conn)
        {
          close(fds[i].fd);
          fds[i].fd = -1;
          compress_array = TRUE;
        }

       
      }  /* End of existing connection is readable             */
   
    } /* End of loop through pollable descriptors              */

    /***********************************************************/
    /* If the compress_array flag was turned on, we need       */
    /* to squeeze together the array and decrement the number  */
    /* of file descriptors. We do not need to move back the    */
    /* events and revents fields because the events will always*/
    /* be POLLIN in this case, and revents is output.          */
    /***********************************************************/
    if (compress_array)
    {
      compress_array = FALSE;
      for (i = 0; i < nfds; i++)
      {
        if (fds[i].fd == -1)
        {
          for(j = i; j < nfds; j++)
          {
            fds[j].fd = fds[j+1].fd;
          }
          i--;
          nfds--;
       
        }
      }
    }



   



  } while (end_server == FALSE); /* End of serving running.    */

  /*************************************************************/
  /* Clean up all of the sockets that are open                 */
  /*************************************************************/
  for (i = 0; i < nfds; i++)
  {
    if(fds[i].fd >= 0)
      close(fds[i].fd);
  }
}




void connmgr_free()
{    
dpl_free(&list,true);
}
