/**
 * The MapReduce coordinator.
 */

#ifndef H1_H__
#define H1_H__
#include "../rpc/rpc.h"
#include "../lib/lib.h"
#include "../app/app.h"
#include "job.h"
#include <time.h>
#include <glib.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <rpc/pmap_clnt.h>
#include <string.h>
#include <memory.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>

typedef struct {

  /* Next job ID to be assigned, starting from 0 */
  int next_jid;

  /* A hashmap that maps jid to a job structure */
  GHashTable* job_ht;

  /* Job waiting queue */
  GList* job_wq;

} coordinator;

void coordinator_init(coordinator** coord_ptr);
#endif
