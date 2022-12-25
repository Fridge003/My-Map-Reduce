/**
 * Logic for job and task management.
 *
 * You are not required to modify this file.
 */

#include "job.h"

/* Check whether a job has finished all map tasks. */
bool map_finished(job_t* job) {
    int n_map = job->n_map;
    for (int i = 0; i < n_map; ++i) {
        if (job->map_task[i] != TASK_FINISHED) {
            return false;
        }
    }
    return true;
}

/* Check whether a job has finished all reduce tasks. */
bool reduce_finished(job_t* job) {
    if (!map_finished(job)) {
        return false;
    }
    int n_reduce = job->n_reduce;
    for (int i = 0; i < n_reduce; ++i) {
        if (job->reduce_task[i] != TASK_FINISHED) {
            return false;
        }
    }   
    return true;
}

/* Search for the next available task in a job, and store it in a task_t pointer. */
/* While searching through the task lists, we also check whehter undergoing worker is crashed. */
void next_task (job_t* job, task_t* task) {
    task->available = true;
    int n_map = job->n_map;
    int n_reduce = job->n_reduce;
    for (int i = 0; i < n_map; ++i) {

        /* Detect a crashed worker. */
        if(job->map_task[i] == TASK_UNDERGOING) {
            if(time(NULL) - (job->map_task_time[i]) > TASK_TIMEOUT_SECS) {
                job->map_task[i] = TASK_IDLE;
            }
        }

        if(job->map_task[i] == TASK_IDLE) {
            task->map = true;
            task->task_id = i;
            job->map_task[i] = TASK_UNDERGOING;
            job->map_task_time[i] = time(NULL);
            return;
        }
    }

    /* Only assign reduce tasks after all map tasks have been finished. */
    if(map_finished(job)) {
        for (int j = 0; j < n_reduce; ++j) {

            /* Detect a crashed worker. */
            if(job->reduce_task[j] == TASK_UNDERGOING) {
                if(time(NULL) - (job->reduce_task_time[j]) > TASK_TIMEOUT_SECS) {
                    job->reduce_task[j] = TASK_IDLE;
                }
            }

            if(job->reduce_task[j] == TASK_IDLE) {
                task->map = false;
                task->task_id = j;
                job->reduce_task[j] = TASK_UNDERGOING;
                job->reduce_task_time[j] = time(NULL);
                return;
            }
        }
    }
    task->available = false;
    task->map = false;
    task->task_id = -1;
    return;
}

/* Clean a job data structure. */
void free_job(job_t* job) {
    if(job->app) {free(job->app);}
    if(job->output_dir) {free(job->output_dir);}
    if(job->args.args_len > 0) {free(job->args.args_val);}
    if(job->map_task) {free(job->map_task);}
    if(job->reduce_task) {free(job->reduce_task);}
    if(job->map_task_time) {free(job->map_task_time);}
    if(job->reduce_task_time) {free(job->reduce_task_time);}
    for (int i = 0; i < job->n_map; ++i) {
        if(job->input_files[i]) {
            free(job->input_files[i]);
        }
    }
    free(job->input_files);
    free(job);
}