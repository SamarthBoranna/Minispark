#include "minispark.h"

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
    maxpartitions = max(maxpartitions, dep->partitions->size);
  }
  va_end(args);

  rdd->numdependencies = numdeps;
  rdd->trans = t;
  rdd->fn = fn;
  rdd->partitions = NULL;
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
  rdd->partitions = list_init(numpartitions);
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
  rdd->partitions = list_init(numfiles);

  for (int i = 0; i < numfiles; i++)
  {
    FILE *fp = fopen(filenames[i], "r");
    if (fp == NULL) {
      perror("fopen");
      exit(1);
    }
    list_add_elem(rdd->partitions, fp);
  }

  rdd->numdependencies = 0;
  rdd->trans = MAP;
  rdd->fn = (void *)identity;
  return rdd;
}

void execute(RDD* rdd) {
  
  for(int i = 0; i<rdd->numdependencies; i++){
    execute(rdd->dependencies[i]);
  }
  
  switch(rdd->trans){
    case MAP:
    case FILTER:
    case JOIN:
    case PARTITIONBY:
    case FILE_BACKED:
  }
  return;
}

void MS_Run() {
  
  global_pool = initThreadPool()
  return;
}

void MS_TearDown() {
  return;
}

int count(RDD *rdd) {
  execute(rdd);

  int count = 0;
  // count all the items in rdd
  return count;
}

void print(RDD *rdd, Printer p) {
  execute(rdd);

  // print all the items in rdd
  // aka... `p(item)` for all items in rdd
}

//////// Thread Pool Actions ////////

ThreadPool *initThreadPool(int numthreads){
  ThreadPool *tpool = malloc(sizeof(ThreadPool));
  if(tpool == NULL){
    perror("malloc");
    exit(1); 
  }

  tpool->numThreads = numthreads;
  tpool->stop = 0;

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
  initQueue(tpool->queue);
  pthread_mutex_init(&tpool->work_lock, NULL);

  for(int i = 0; i<numthreads; i++){
    pthread_create(&tpool->threads[i], NULL, worker, tpool);
  }

  pthread_cond_init(&tpool->toBeDone, NULL);
  pthread_cond_init(&tpool->waiting, NULL); 
  tpool->activeTasks = 0;

  return tpool;
}

void thread_pool_destroy(){

  pthread_mutex_lock(&global_pool->work_lock);
  global_pool->stop = 1;
  pthread_cond_broadcast(&global_pool->toBeDone);
  pthread_mutex_unlock(&global_pool->work_lock);

  for(int i = 0; i<global_pool->numThreads; i++){
    int join_ret = pthread_join(global_pool->threads[i], NULL);
    //error?
  }

  free(global_pool->queue);
  pthread_mutex_destroy(&global_pool->work_lock);
  pthread_cond_destroy(&global_pool->toBeDone);
  free(global_pool->threads);
  free(global_pool);
  global_pool = NULL;
}

void thread_pool_wait(){
  pthread_mutex_lock(&global_pool->work_lock);
  TaskQueue *queue = global_pool->queue;
  while(queue->size != 0 || global_pool->activeTasks != 0){
    pthread_cond_wait(&global_pool->waiting, &global_pool->work_lock);
  }
  pthread_cond_signal(&global_pool->waiting);
  pthread_mutex_unlock(&global_pool->work_lock);
}

int thread_pool_submit(ThreadPool* tpool, Task* task){
  int push_val = push(tpool->queue, task);
  if(push_val == 0){
    pthread_mutex_lock(&tpool->work_lock);
    pthread_cond_signal(&tpool->toBeDone);
    pthread_mutex_unlock(&tpool->work_lock);
  }
  return push_val;
}

void *worker(void *argument){
  ThreadPool *tpool = (ThreadPool *)argument;

  while(1){
    pthread_mutex_lock(&tpool->work_lock);
    TaskQueue *queue = tpool->queue;
    while(queue->size == 0 && tpool->stop == 0){
      pthread_cond_wait(&tpool->toBeDone, &tpool->work_lock);
    }

    if(tpool->stop){
      pthread_mutex_unlock(&tpool->work_lock);
      break;
    }

    Task *taskToBeDone = pop(queue);
    tpool->activeTasks += 1;
    pthread_mutex_unlock(&tpool->work_lock);

    if(taskToBeDone != NULL){
      //process task
      pthread_mutex_lock(&tpool->work_lock);
      tpool->activeTasks -= 1;
      if(queue->size == 0 && tpool->activeTasks == 0){
        pthread_cond_signal(&tpool->waiting);
      }
      pthread_mutex_unlock(&tpool->work_lock);
    }
    return NULL;

  }
}
//////// List Actions ////////

List *initList(List *list){
  list->head = NULL;
  list->tail = NULL;
  list->size = 0;
  return list;
}

int append(List *list, node *new_node){
  new_node->next = NULL;
  if(list->head == NULL){
    list->head = new_node;
    list->tail = new_node;
  }
  else{
    node *tail = list->tail;
    tail->next = new_node;
    list->tail = new_node;
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
  pthread_mutex_init(&queue->lock, NULL);
}

Task *pop(TaskQueue *queue){
  pthread_mutex_lock(&queue->lock);
  if(queue == NULL || queue->head == NULL){
    pthread_mutex_unlock(&queue->lock);
    return NULL;
    //error?
  }

  Task *popped = queue->head;
  Task *next = popped->next;
  queue->head = next;
  queue->size -= 1;
  pthread_mutex_unlock(&queue->lock);

  return popped;
}

int push(TaskQueue *queue, Task *taskToPush){
  taskToPush->next = NULL;
  pthread_mutex_lock(&queue->lock);
  if(queue->head == NULL){
    queue->head = taskToPush;
    queue->tail = taskToPush;
  }
  else{
    Task *tail = queue->tail;
    tail->next = taskToPush;
    queue->tail = taskToPush;
  }
  queue->size += 1;
  pthread_mutex_unlock(&queue->lock);
  return 0;
}
