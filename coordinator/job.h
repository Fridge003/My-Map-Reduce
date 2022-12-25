/**
 * Logic for job and task management.
 *
 * You are not required to modify this file.
 */

#include "../lib/lib.h"
#include <stdbool.h>
#include <time.h>

#ifndef JOB_H__
#define JOB_H__


/* Macros that define the status of a map/reduce task. */
#define TASK_FINISHED 0
#define TASK_IDLE 1
#define TASK_UNDERGOING 2

/* The data structure of a task. */
typedef struct task{
    int task_id;
    bool map; /* True: a map task, False: a reduce task. */
    bool available; /* True: there is no available task for this job. */
}task_t;



/* The data structure of a job. */
typedef struct job {
    int jid;
    char* app;
    char* output_dir;
    char** input_files;
    int n_map;
    int n_reduce;
	struct {
		unsigned int args_len;
		char* args_val;
	} args;
    int* map_task; /* Should have n_map elements */
    int* reduce_task; /* Should have n_reduce elements */
    time_t* map_task_time; /* Should have n_map elements */
    time_t* reduce_task_time; /* Should have n_reduce elements */
    bool done;
    bool failed;
} job_t;

/* Check whether a job has finished all map tasks. */
bool map_finished(job_t* job);

/* Check whether a job has finished all reduce tasks. */
bool reduce_finished(job_t* job);

/* Search for the next available task in a job, and store it in a task pointer. */
void next_task(job_t* job, task_t* task);

/* Clean a job data structure. */
void free_job(job_t* job);

#endif
