/**
 * The MapReduce coordinator.
 */

#include "coordinator.h"
#include "job.h"

#ifndef SIG_PF
#define SIG_PF void (*)(int)
#endif

/* Global coordinator state. */
coordinator* state;

extern void coordinator_1(struct svc_req*, struct SVCXPRT*);

/* Set up and run RPC server. */
int main(int argc, char** argv) {
  register SVCXPRT* transp;

  pmap_unset(COORDINATOR, COORDINATOR_V1);

  transp = svcudp_create(RPC_ANYSOCK);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create udp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_UDP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, udp).");
    exit(1);
  }

  transp = svctcp_create(RPC_ANYSOCK, 0, 0);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create tcp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_TCP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, tcp).");
    exit(1);
  }

  coordinator_init(&state);

  svc_run();
  fprintf(stderr, "%s", "svc_run returned");
  exit(1);
  /* NOTREACHED */
}

/* EXAMPLE RPC implementation. */
int* example_1_svc(int* argp, struct svc_req* rqstp) {
  static int result;

  result = *argp + 1;

  return &result;
}

/* SUBMIT_JOB RPC implementation. */
int* submit_job_1_svc(submit_job_request* argp, struct svc_req* rqstp) {
  static int result;

  printf("Received submit job request\n");

  /* First validate app name */
  if(get_app(argp->app).name == NULL) {
    result = -1;
    return &result;
  }

  /* Create a new job, and initialize. */
  job_t* job = (job_t*)malloc(sizeof(job_t));
  job->jid = state->next_jid++;
  job->app = strdup(argp->app);
  job->output_dir = strdup(argp->output_dir);
  job->n_map = argp->files.files_len;
  job->n_reduce = argp->n_reduce;
  job->args.args_len = argp->args.args_len;
  if(argp->args.args_len > 0) {
    job->args.args_val = strdup(argp->args.args_val);
  }
  job->done = false;
  job->failed = false;
  job->input_files = (char**)malloc(sizeof(char*) * (job->n_map));
  for (int i = 0; i < job->n_map; ++i) {
    job->input_files[i] = strdup(argp->files.files_val[i]);
  }
  job->map_task = (int*)malloc(sizeof(int) * (job->n_map));
  job->reduce_task = (int*)malloc(sizeof(int) * (job->n_reduce));
  job->map_task_time = (time_t*)malloc(sizeof(time_t) * (job->n_map));
  job->reduce_task_time = (time_t*)malloc(sizeof(time_t) * (job->n_reduce));
  for(int i = 0; i < job->n_map; ++i) {
    job->map_task[i] = TASK_IDLE;
    job->map_task_time[i] = 0;
  }
  for(int i = 0; i < job->n_reduce; ++i) {
    job->reduce_task[i] = TASK_IDLE;
    job->reduce_task_time[i] = 0;
  }

  /* Insert jid-job pair to hash table. */
  g_hash_table_insert(state->job_ht, GINT_TO_POINTER(job->jid), job);
  
  /* Insert jid to job waiting list. */
  state->job_wq = g_list_append(state->job_wq, GINT_TO_POINTER(job->jid));

  /* Set result. */
  result = job->jid;

  /* For Testing. */
  /*
  printf("Successfully push job into waiting queue: \n");
  printf("JID: %d\napp: %s\noutput_dir: %s\n", job->jid, job->app, job->output_dir);
  printf("n_map: %d\nn_reduce: %d\n", job->n_map, job->n_reduce);
  if(job->args.args_len > 0) {
    printf("Args: %s\n", job->args.args_val);
  }
  printf("Input files: ");
  for(int i = 0; i < job->n_map; ++i) {
    printf("%s ", job->input_files[i]);
  }
  */

  /* Do not modify the following code. */
  /* BEGIN */
  struct stat st;
  if (stat(argp->output_dir, &st) == -1) {
    mkdirp(argp->output_dir);
  }

  return &result;
  /* END */

}

/* POLL_JOB RPC implementation. */
poll_job_reply* poll_job_1_svc(int* argp, struct svc_req* rqstp) {
  static poll_job_reply result;

  //printf("Received poll job request\n");

  int jid = *argp;
  job_t* job = g_hash_table_lookup(state->job_ht, GINT_TO_POINTER(jid));

  result.done = false;
  result.failed = false;
  result.invalid_job_id = false;

  /* If jid unfound in hash table, set invalid_job_id to false. */
  if (job == NULL) {
    result.invalid_job_id = true;
  } else {
  /* Else pick information from job structure. */
    result.done = job->done;
    result.failed = job->failed;
  }

  return &result;
}



/* GET_TASK RPC implementation. */
get_task_reply* get_task_1_svc(void* argp, struct svc_req* rqstp) {
  static get_task_reply result;

  //printf("Received get task request\n");
  result.file = "";
  result.output_dir = "";
  result.app = "";
  result.wait = true;
  result.args.args_len = 0; 

  /* The job waiting list is empty, return directly. */
  if(state->job_wq == NULL) {
    return &result;
  }


  bool no_available_task = true;
  GList* elem;
  int jid;
  job_t* job;
  task_t task;

  /* Iterate through the list to detect crashed worker. */
  

  /* Iterate through the list to search for available tasks. */
  for (GList* elem = state->job_wq; elem; elem = elem->next) {
    jid = GPOINTER_TO_INT(elem->data); 
    job = g_hash_table_lookup(state->job_ht, GINT_TO_POINTER(jid));
    next_task(job, &task);
    if(task.available) {
      no_available_task = false;
      break;
    }
  }

  /* Can't find idle task for execution. */
  if(no_available_task) {
    return &result;
  }

  /* A task found. */
  result.job_id = jid;
  result.task = task.task_id;
  result.reduce = !task.map;
  if(!result.reduce) {
    result.file = strdup(job->input_files[task.task_id]);
  }
  result.wait = false;
  result.n_map = job->n_map;
  result.n_reduce = job->n_reduce;
  result.app = strdup(job->app);
  result.output_dir = strdup(job->output_dir);
  result.args.args_len = job->args.args_len;
  if(result.args.args_len > 0) {
    result.args.args_val = strdup(job->args.args_val);
  }

  /* For Testing. */
  printf("Successfully fetching a task: \n");
  printf("TaskID: %d, Reduce: %d, Input File: %s\n", result.task, result.reduce, result.file);

  return &result;
}

/* FINISH_TASK RPC implementation. */
void* finish_task_1_svc(finish_task_request* argp, struct svc_req* rqstp) {
  static char* result;

  // printf("Received finish task request\n");
  job_t* job = g_hash_table_lookup(state->job_ht, GINT_TO_POINTER(argp->job_id));
  
  /* If success, set task status and check whether the job has been done. */
  /* If failed, set failed=true for this job and remove it from waiting list. */
  if(argp->success) {
    if(!argp->reduce) { /* A Map task. */
      job->map_task[argp->task] = TASK_FINISHED;
      printf("Job %d finished map task %d.\n", job->jid, argp->task);
    } else { /* A Reduce task. */
      job->reduce_task[argp->task] = TASK_FINISHED;
      printf("Job %d finished reduce task %d.\n", job->jid, argp->task);
      if(reduce_finished(job)) {
        job->done = true;
        job->failed = false;
        state->job_wq = g_list_remove(state->job_wq, GINT_TO_POINTER(job->jid));
        printf("Job %d finished.\n", job->jid);
      }
    }
  } else {
    job->done = true;
    job->failed = true;
    state->job_wq = g_list_remove(state->job_wq, GINT_TO_POINTER(job->jid));
    printf("Job %d Failed when processing reduce: %d, task: %d.\n", job->jid, argp->reduce, argp->task);
  }

  return (void*)&result;
}


/* Initialize coordinator state. */
void coordinator_init(coordinator** coord_ptr) {
  *coord_ptr = malloc(sizeof(coordinator));
  coordinator* coord = *coord_ptr;
  coord->next_jid = 0;
  coord->job_ht = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
  coord->job_wq = NULL;
}


