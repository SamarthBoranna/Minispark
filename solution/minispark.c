#include "minispark.h"
#define _GNU_SOURCE
static ThreadPool *global_pool = NULL;

// Working with metrics...
// Recording the current time in a `struct timespec`:
//    clock_gettime(CLOCK_MONOTONIC, &metric->created);
// Getting the elapsed time in microseconds between two timespecs:
//    duration = TIME_DIFF_MICROS(metric->created, metric->scheduled);
// Use `print_formatted_metric(...)` to write a metric to the logfile. 
void print_formatted_metric(TaskMetric* metric, FILE* fp) {
  fprintf(fp, "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
	  metric->rdd, metric->pnum, metric->rdd->trans,
	  metric->created.tv_sec, metric->created.tv_nsec / 1000,
	  metric->scheduled.tv_sec, metric->scheduled.tv_nsec / 1000,
	  metric->duration);
}

int max(int a, int b)
{
  return a > b ? a : b;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
  RDD *rdd = malloc(sizeof(RDD));
  if (rdd == NULL)
  {
    printf("error mallocing new rdd\n");
    exit(1);
  }

  va_list args;
  va_start(args, fn);

  int maxpartitions = 0;
  for (int i = 0; i < numdeps; i++)
  {
    RDD *dep = va_arg(args, RDD *);
    rdd->dependencies[i] = dep;
    maxpartitions = max(maxpartitions, dep->numpartitions);
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
  rdd->numpartitions = maxpartitions;
  rdd->materialized = 0;
  return rdd;
}

/* RDD constructors */
RDD *map(RDD *dep, Mapper fn)
{
  return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
  RDD *rdd = create_rdd(1, FILTER, fn, dep);
  rdd->ctx = ctx;
  return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
  RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
  rdd->numpartitions = numpartitions;
  rdd->ctx = ctx;
  return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
  RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
  rdd->ctx = ctx;
  return rdd;
}

/* A special mapper */
void *identity(void *arg)
{
  return arg;
}

/* Special RDD constructor.
 * By convention, this is how we read from input files. */
RDD *RDDFromFiles(char **filenames, int numfiles)
{
  RDD *rdd = malloc(sizeof(RDD));
  List* list = malloc(sizeof(List));
  rdd->partitions = list_init(list);

  for (int i = 0; i < numfiles; i++) {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL) {
      perror("fopen");
      exit(1);
    }

    List* inner_list = malloc(sizeof(List));
    inner_list = list_init(inner_list);
    node* file_node = malloc(sizeof(node));
    if (file_node == NULL) {
      perror("malloc");
      fclose(fp);
      exit(1);
    }
    file_node->data = fp;
    append(inner_list, file_node);

    node *part_node = malloc(sizeof(node));
    part_node->data = inner_list;
    append(rdd->partitions, part_node);
  }

  rdd->numdependencies = 0;
  rdd->numpartitions = numfiles;
  rdd->trans = FILE_BACKED;  //might have to switch to FILE_BACKED and have switch case in materialize
  rdd->fn = (void *)identity;
  return rdd;
}

void execute(RDD* rdd) {

  // Skip already materialized RDDs
  if (rdd->materialized) return;

  // Materialize dependencies
  for (int i = 0; i < rdd->numdependencies; i++) {
    execute(rdd->dependencies[i]);
  }

  // Determine num partitions
  int num_partitions = 1;
  // Transformations
  if (rdd->numdependencies > 0) {
    if (rdd->trans != PARTITIONBY)
      num_partitions = rdd->dependencies[0]->numpartitions;
    else
      num_partitions = rdd->numpartitions;
  }
  // File-backed RDDs
  else if (rdd->trans == FILE_BACKED || rdd->partitions != NULL) {
    num_partitions = rdd->numpartitions;
  }

  // Init partitions
  if (rdd->partitions == NULL) {
    List* list = malloc(sizeof(List));
    if (list == NULL) {
      perror("malloc");
      exit(1);
    }
    rdd->partitions = list_init(list); 
    
    // Initialize each partition as an empty list??
    for (int i = 0; i < num_partitions; i++) {
      List* partition = malloc(sizeof(List));
      if (partition == NULL) {
        perror("malloc");
        exit(1);
      }
      list_init(partition);

      node* partition_node = malloc(sizeof(node));
      if (partition_node == NULL) {
        perror("malloc");
        exit(1);
      }
      partition_node->data=partition;
      append(rdd->partitions, partition_node);
    }
  }

  for (int i = 0; i < num_partitions; i++) {
    Task* task = malloc(sizeof(Task));
    TaskMetric* task_metric = malloc(sizeof(TaskMetric));
    if (task == NULL) {
      perror("malloc");
      exit(1);
    }
    task->rdd = rdd;
    task->pnum = i;
    
    task->metric = task_metric;
    task->metric->rdd = rdd;
    task->metric->pnum = i;
    clock_gettime(CLOCK_MONOTONIC, &task->metric->created);

    // add the tasks to the task queue 
    thread_pool_submit(global_pool, task);
  }

  thread_pool_wait();
  rdd->materialized = 1;
}

void materialize(RDD* rdd, int pnum) {
  node* partition_node = getList(rdd->partitions, pnum);
  List* partition = (List *)partition_node->data; 

  switch (rdd->trans) {
    case MAP: {
      if (rdd->numdependencies > 0) {
        RDD* dep = rdd->dependencies[0];
        node* dep_partition_node = getList(dep->partitions, pnum);
        List* dep_partition = (List *)dep_partition_node->data;
        Mapper map_fn = (Mapper)rdd->fn;

        // Apply map to all nodes
        node* curr = seek_from_start(dep_partition);
        while (curr != NULL) {
          void* result = map_fn(curr->data);
          if(dep->trans == FILE_BACKED){
            while(result != NULL) {
              node* new_node = malloc(sizeof(node));
              new_node->data = result;
              append(partition, new_node);
              result = map_fn(curr->data);
            }
          }
          else{
            if(result != NULL){
              node* new_node = malloc(sizeof(node));
              new_node->data = result;
              append(partition, new_node);
            }
          }
          curr = nextList(curr);
        }
      }
      // Otherwise its a FILE-BACKED RDD
      break;
    }

    case FILE_BACKED: {
      break;
  }

    case FILTER: {
      RDD* dep = rdd->dependencies[0];
      node* dep_partition_node = getList(dep->partitions, pnum);
      List* dep_partition = (List *)dep_partition_node->data;
      Filter filter_fn = (Filter)rdd->fn;
      void* ctx = rdd->ctx;

      // Apply filter to all nodes
      node* curr = seek_from_start(dep_partition);
      while (curr != NULL) {
        if (filter_fn(curr->data, ctx)) { // filter the current node's data
          node* new_node = malloc(sizeof(node));
          new_node->data = curr->data;
          append(partition, new_node);
        }
        curr = nextList(curr);
      }
      break;
    }

    case JOIN: {
      RDD* dep1 = rdd->dependencies[0];
      node* dep1_partition_node = getList(dep1->partitions, pnum);
      List* dep1_partition = (List *)dep1_partition_node->data;

      RDD* dep2 = rdd->dependencies[1];
      node* dep2_partition_node = getList(dep2->partitions, pnum);
      List* dep2_partition = (List *)dep2_partition_node->data;

      Joiner join_fn = (Joiner)rdd->fn;
      void* ctx = rdd->ctx;

      // Apply inner join
      node* curr1 = seek_from_start(dep1_partition);
      while (curr1 != NULL) {
        node* curr2 = seek_from_start(dep2_partition);
        while (curr2 != NULL) {
          void* result = join_fn(curr1->data, curr2->data, ctx);
          if (result != NULL) {
            node* new_node = malloc(sizeof(node));
            new_node->data = result;
            append(partition, new_node);
          }
          curr2 = nextList(curr2);
        }
        curr1 = nextList(curr1);
      }
      break;
    }

    case PARTITIONBY: {
      RDD* dep = rdd->dependencies[0];
      Partitioner partitioner_fn = (Partitioner)rdd->fn;
      void* ctx = rdd->ctx;

      // Apply partition function
      for (int i = 0; i < dep->partitions->size; i++) {
        node* dep_partition_node = getList(dep->partitions, i);
        List* dep_partition = (List *)dep_partition_node->data;
        
        node* curr = seek_from_start(dep_partition);
        while (curr != NULL) {
          // Make sure to partition number matches
          unsigned long result_partition = partitioner_fn(curr->data, rdd->numpartitions, ctx);          
          if ((int) result_partition == pnum) {
            node* new_node = malloc(sizeof(node));
            new_node->data = curr->data;
            append(partition, new_node);
          }
          curr = nextList(curr);
        }
      }
      break;
    }
  }
}

void MS_Run() {
  cpu_set_t set;
  CPU_ZERO(&set);

  if (sched_getaffinity(0, sizeof(set), &set) == -1) {
    perror("sched_getaffinity");
    exit(1);
  }

  int numthreads = CPU_COUNT(&set);

  global_pool = initThreadPool(numthreads);
  return;
}

void MS_TearDown() {
  thread_pool_wait();
  thread_pool_destroy();
  //need to free RDD somehow
  return;
}

int count(RDD *rdd) {
  execute(rdd);

  int count = 0;
  // count all the items in rdd
  for(int i = 0; i < rdd->partitions->size; i++) {
    node* partition_node = getList(rdd->partitions, i);
    List* partition = (List *)partition_node->data;
    count += partition->size;
  }
  return count;
}

void print(RDD *rdd, Printer p) {
  execute(rdd);
  // print all the items in rdd
  // aka... `p(item)` for all items in rdd
  for (int i = 0; i < rdd->partitions->size; i++) {
    node* partition_node = getList(rdd->partitions, i);
    List* partition = (List *)partition_node->data;
    node* curr = seek_from_start(partition);
    while (curr != NULL) {
      p(curr->data);
      curr = nextList(curr);
    }
  }
}

//////// List Actions ////////

List *list_init(List *list){
  list->head = NULL;
  list->tail = NULL;
  list->size = 0;
  return list;
}

int append(List *list, void *new_node){
  node* n = (node *)new_node;
  n->next = NULL;
  if(list->head == NULL){
    list->head = n;
    list->tail = n;
  }
  else{
    node *tail = list->tail;
    tail->next = n;
    list->tail = n;
  }
  list->size += 1;
  return 0;
}

node *getList(List *list, int index){
  if(index < 0 || index >= list->size){
    return NULL;
  }
  node *curr = list->head;
  for(int i = 0; i<index; i++){
    curr = curr->next;
  }

  return curr;
}

node *nextList(node *curr_node){
  if(curr_node->next == NULL){
    return NULL;
  }
  else return curr_node->next; 
}

node *seek_from_start(List *list){
  if(list->head == NULL){
    return NULL;
  }
  return list->head;
}

void freeList(List *list){
  while(list->head != NULL){
    node *curr = list->head;
    list->head = curr->next;
    free(curr);
  }
  free(list);
}
//////// task queue actions ////////

void initQueue(TaskQueue* queue) {
  queue->head = NULL;
  queue->tail = NULL;
  queue->size = 0;
}



void freeQueue(Task *head) {
  Task *current = head;
  while (current != NULL) {
    Task *next = current->next;
    free(current);
    current = next;
  }
}

//////// Free RDD ////////
void freeRDD(RDD *rdd){
  if (rdd == NULL) return;
  for (int i = 0; i < rdd->numdependencies; i++) {
    freeRDD(rdd->dependencies[i]);
  }
  if (rdd->partitions != NULL) {
    freeList(rdd->partitions);
  }
  free(rdd);
}

//////// Task Metric Funcs ///////////

TaskMetricQueue *initTMQ(TaskMetricQueue* TMQueue){
  TMQueue->head = NULL;
  TMQueue->tail = NULL;
  TMQueue->size = 0;
  pthread_cond_init(&TMQueue->TMQ_wait, NULL);
  pthread_mutex_init(&TMQueue->TMQ_lock, NULL);

  return TMQueue;
}

TaskMetric *TMQpop(TaskMetricQueue *TMqueue){
  if(TMqueue == NULL || TMqueue->head == NULL){
    return NULL;
    //error?
  }
  TaskMetric *popped = TMqueue->head;
  TaskMetric *next = popped->next;
  TMqueue->head = next;
  TMqueue->size -= 1;

  return popped;
}

int TMQpush(TaskMetricQueue *TMqueue, TaskMetric *TM_ToPush){
  TM_ToPush->next = NULL;
  if(TMqueue->head == NULL){
    TMqueue->head = TM_ToPush;
    TMqueue->tail = TM_ToPush;
  }
  else{
    TaskMetric *tail = TMqueue->tail;
    tail->next = TM_ToPush;
    TMqueue->tail = TM_ToPush;
  }
  TMqueue->size += 1;
  return 0;
}

//////// Thread Pool Actions ////////


void *worker(void *arg){
  ThreadPool *tpool = arg;

  while (1) {
    pthread_mutex_lock(&tpool->work_lock);
      while (tpool->queue->size == 0 && tpool->stop == 0) {
        pthread_cond_wait(&tpool->toBeDone, &tpool->work_lock);
      }
      if (tpool->stop == 1) {
        pthread_mutex_unlock(&tpool->work_lock);
        break;
      }
      //POP
      Task *task = tpool->queue->head;
      tpool->queue->head = task->next;
      if (tpool->queue->head == NULL)
        tpool->queue->tail = NULL;
      tpool->queue->size--;
      tpool->activeTasks++;
      //END POP

      // Grab task_metric from Metric queue
      clock_gettime(CLOCK_MONOTONIC, &task->metric->scheduled); // Getting the scheduled time
    pthread_mutex_unlock(&tpool->work_lock);

    materialize(task->rdd, task->pnum);
    free(task);

    pthread_mutex_lock(&tpool->work_lock);
      struct timespec time_complete;
      clock_gettime(CLOCK_MONOTONIC, &time_complete);
      task->metric->duration = TIME_DIFF_MICROS(task->metric->scheduled, time_complete);
      TMQpush(tpool->TMqueue, task->metric);
      pthread_cond_signal(&tpool->TMqueue->TMQ_wait);
      
      tpool->activeTasks--;
      if (tpool->queue->size == 0 && tpool->activeTasks == 0)
        pthread_cond_signal(&tpool->waiting);
    pthread_mutex_unlock(&tpool->work_lock);
  }
  return NULL;
}

void *monitor(void *arg){
  TaskMetricQueue *TMQ = arg;
  pthread_mutex_lock(&TMQ->TMQ_lock);
  FILE *fp = fopen("metrics.log", "w");
  if(fp == NULL){
    fclose(fp);
    return NULL;
  }
  while(1){
    while((TMQ->head == NULL || TMQ->size == 0) && global_pool->stop == 0){
      pthread_cond_wait(&TMQ->TMQ_wait, &TMQ->TMQ_lock);
    }
    if(global_pool->stop == 1 && TMQ->head == NULL){
      break;
    }
    TaskMetric *tm = TMQpop(TMQ);
    if(tm != NULL){
      print_formatted_metric(tm, fp);
    }

  }
  fclose(fp);
  pthread_mutex_unlock(&TMQ->TMQ_lock);
  return NULL;
}

ThreadPool *initThreadPool(int numthreads){
  ThreadPool *tpool = malloc(sizeof(ThreadPool));
  if(tpool == NULL){
    perror("malloc");
    exit(1); 
  }

  tpool->numThreads = numthreads;
  tpool->stop = 0;
  tpool->activeTasks = 0;

  tpool->threads = malloc(numthreads * sizeof(pthread_t));

  if(tpool->threads == NULL){
    perror("malloc");
    exit(1);
  }

  tpool->queue = malloc(sizeof(TaskQueue));
  if(tpool->queue == NULL){
    perror("malloc");
    exit(1);
  }

  tpool->TMqueue = malloc(sizeof(TaskMetricQueue));

  initQueue(tpool->queue);
  initTMQ(tpool->TMqueue);
  pthread_mutex_init(&tpool->work_lock, NULL);
  pthread_cond_init(&tpool->toBeDone, NULL);
  pthread_cond_init(&tpool->waiting, NULL); 

  for(int i = 0; i<numthreads; i++){
    if (i == 0) {
      pthread_create(&tpool->threads[i], NULL, monitor, tpool->TMqueue);
    }
    else 
      pthread_create(&tpool->threads[i], NULL, worker, tpool);
  }

  return tpool;
}

void thread_pool_destroy(){

  pthread_mutex_lock(&global_pool->work_lock);
  global_pool->stop = 1;
  pthread_cond_broadcast(&global_pool->toBeDone);
  pthread_cond_signal(&global_pool->TMqueue->TMQ_wait);
  pthread_mutex_unlock(&global_pool->work_lock);
 
  for(int i = 0; i<global_pool->numThreads; i++){
    pthread_join(global_pool->threads[i], NULL);
    //error?
  }
  TaskQueue *queue = global_pool->queue;
  freeQueue(queue->head);
  free(global_pool->queue);
  pthread_mutex_destroy(&global_pool->work_lock);
  pthread_cond_destroy(&global_pool->toBeDone);
  pthread_cond_destroy(&global_pool->waiting);
  free(global_pool->threads);
  free(global_pool);
  global_pool = NULL;
}

void thread_pool_wait(){
  pthread_mutex_lock(&global_pool->work_lock);
  TaskQueue *queue = global_pool->queue;
  while(queue->size > 0 || global_pool->activeTasks > 0){
    pthread_cond_wait(&global_pool->waiting, &global_pool->work_lock);
  }
  pthread_mutex_unlock(&global_pool->work_lock);
}

int thread_pool_submit(ThreadPool *tpool, Task *task) {
  pthread_mutex_lock(&tpool->work_lock);

  task->next = NULL;
  if (tpool->queue->head == NULL) {
    tpool->queue->head = task;
    tpool->queue->tail = task;
  } else {
    tpool->queue->tail->next = task;
    tpool->queue->tail = task;
  }
  tpool->queue->size++;

  pthread_cond_signal(&tpool->toBeDone);
  pthread_mutex_unlock(&tpool->work_lock);
  return 0;
}


