#include "minispark.h"

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
  List* list = malloc(sizeof(List));
  rdd->partitions = list_init(list);

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
    rdd->partitions = list_init(list); 
    
    // Initialize each partition as an empty list??
    for (int i = 0; i < num_partitions; i++) {
      List* partition = malloc(sizeof(List));
      list_init(partition);

      node* partition_node = malloc(sizeof(node));
      partition_node->data=partition;
      list_add_elem(rdd->partitions, partition_node);
    }
  }

  for (int i = 0; i < rdd->numpartitions; i++) {
    Task* task = malloc(sizeof(Task));
    task->rdd = rdd;
    task->pnum = i;
    // add the tasks to the task queue 

    // LOCK
    push(taskQueue, task) // need to initialize queue
    // UNLOCK
  }

  rdd->materialized = 1;
  return;
}

void materialize(RDD* rdd, int pnum) {
  node* partition_node = getList(rdd->partitions, pnum);
  List* partition = partition_node->data;

  switch (rdd->trans) {
    case MAP: {
      if (rdd->numdependencies > 0) {
        RDD* dep = rdd->dependencies[0];
        node* dep_partition_node = getList(dep->partitions, pnum);
        List* dep_partition = dep_partition_node->data;
        Mapper map_fn = (Mapper)rdd->fn;

        // Apply map to all nodes
        node* curr = seek_from_start(dep_partition);
        while (curr != NULL) {
          void* result = map_fn(curr->data);
          if (result != NULL) {
            node* new_node = malloc(sizeof(node));
            new_node->data = result;
            append(partition, new_node);
          }
          curr = nextList(curr);
        }
      }
      // Otherwise its a FILE-BACKED RDD
      break;
    }

    case FILTER: {
      RDD* dep = rdd->dependencies[0];
      node* dep_partition_node = getList(dep->partitions, pnum);
      List* dep_partition = dep_partition_node->data;
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
      List* dep1_partition = dep1_partition_node->data;

      RDD* dep2 = rdd->dependencies[1];
      node* dep2_partition_node = getList(dep2->partitions, pnum);
      List* dep2_partition = dep2_partition_node->data;

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
        List* dep_partition = dep_partition_node->data;
        
        node* curr = seek_from_start(dep_partition);
        while (curr != NULL) {
          // Make sure to partition number matches
          unsigned long result_partition = partitioner_fn(curr->data, rdd->numpartitions, ctx);          
          if (result_partition == pnum) {
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
