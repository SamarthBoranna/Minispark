# Minispark
Distributed data processing frameworks are crucial for performing data
analytics on large quantaties of data. Frameworks like MapReduce and
Spark are powerful and relatively simple to program. Users of Spark
write declarative queries to manipulate data, and Spark processes the
data on distributed clusters, automatically handling difficult
problems of distributed computing -- parallel processing,
inter-process communication, and fault tolerance.

In this project, we will built a mini implementation of Spark using
threads on a single node. Spark represents data processing as a
directed acyclic graph, where the nodes are intermediate
representations of data, known as RDDs, and the edges are
transformations upon the data. Segments of the graph may be operated
on in parallel, and thus a central challenge of this project will be
to understand the data model and how it may be mapped to worker
threads.

Learning objectives:
- To learn about data processing pipelines.
- To implement a correct MiniSpark framework with several common data
  processing operators.
- To efficiently process data in parallel using threads.

## Background

To understand how to make progress on any project that involves
concurrency, you should understand the basics of thread creation,
mutual exclusion (with locks), and signaling/waiting (with condition
variables). These are described in the following book chapters:

- [Intro to Threads](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-intro.pdf)
- [Threads API](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-api.pdf)
- [Locks](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-locks.pdf)
- [Using Locks](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-locks-usage.pdf)
- [Condition Variables](http://pages.cs.wisc.edu/~remzi/OSTEP/threads-cv.pdf)

Read these chapters carefully in order to prepare yourself for this
project.

You may also like to read two fundamental data processing papers which
inspired this project:

- [MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)
- [Resilient Distributed Datasets](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)

Spark is a widely-used system for large-scale data analytics. If this
interests you, you can learn much more about big data processing in
classes like [CS
544](https://tyler.caraza-harter.com/cs544/s25/syllabus.html) or [CS
744](https://pages.cs.wisc.edu/~shivaram/cs744-sp25/).

## General Idea

Data processing pipelines in MiniSpark are organized around the idea
of _transformations_ and _actions_. Let's consider simple examples of
two such transformations -- `map` and `filter`.

```
lst                            = (1 2 3 4)
int  times3(x)                 = (x * 3)
bool even(x)                   = (x % 2 == 0)
map(lst, times3)               : (3 6 9 12)
filter(lst, even)              : (2 4)
filter(map(lst, times3), even) : (6 12)
```

We support two simple actions:
- count: return the number of elements
- print: print each element using a `Printer` function provided by the
  application. In most cases, this just prints to stdout via `printf`.

Continuing the example from above:

```
count(map(lst, times3))               => 4
print(filter(map(lst, times3), even), Printer) => "6\n12\n" [printed to stdout]
```

In MiniSpark, data are represented by immutable objects known as RDDs
(Resilient Distributed Datasets). We use this name in reference to the
original paper (linked above), but beware, our data structures are
neither resilient nor distributed. An RDD can be created from input
files, or via a transformation from another RDD. Thinking of RDDs as
nodes and transformations as edges, the chain of RDDs forms a directed
acyclic graph (DAG). Within each RDD, data is partitioned so that the
transformation between any two RDDs can be computed in parallel. We
represent partitions as lists of data. When RDDs are created, all of
their data partitions are empty. This means we can construct the
entire DAG and defer computing the data (or, _applying the
transformations_) until later.

The process of computing the data within a partition is known as
_materializing_ the partition. To materialize a partition, we apply a
transformation (Map, Filter, Join, or PartitionBy) on each element of
its dependent partition(s) and add the result to the current
partition. We materialize partitions when an action (`count` or
`print`) is performed on the RDD. MiniSpark programs all create a DAG
of RDDs and run an action on top-level RDD of the DAG. Thus, the
central task of this project is to materialize the RDD given as an
argument to `count` or `print`. To do so efficiently, you will need to
schedule threads to:
- materialize partitions of an RDD in parallel
- compute independent parts of the DAG in parallel

What is "each element of its dependent partitions"?
A RDD will have multiple partitions. Until here, it will match your 
intuition from the diagrams. For each partitions, it will have multiple
elements which can be get by calling the applied function until it reaches
NULL. It is similar to the iterator behavior of modern programming 
languages like `__next__()` method in python, or `Iterator` Trait in rust. 

## An example program: Linecount
Here we consider an example program written for the minispark framework.
```
void* GetLines(void* arg) {
  FILE *fp = (FILE*)arg;

  char *line = NULL;
  size_t size = 0;
  
  if (getline(&line, &size, fp) < 0) {
    return NULL;
  }
  return line;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("need at least one file\n");
    return -1;
  }

  MS_Run();
  
  RDD* files = RDDFromFiles(argv + 1, argc - 1);
  printf("total number of lines: %d\n", count(map(files, GetLines)));
  
  MS_TearDown();
  return 0;
}
```

`linecount` counts and prints the number of lines in all the files
passed as arguments. First, `linecount` passes the contents of `argv`
(minus the first element) to `RDDFromFiles`, which creates an RDD with
one partition for each file. Then, `linecount` applies the map
function `GetLines` to each partition in the RDD. `GetLines` reads and
returns a line from a file or `NULL` if it reaches EOF. Lastly,
`linecount` sums the number of lines found with `count` and prints the
result. Here is a figure showing the DAG for linecount:

![SimpleExample](graphics/getline.png)

In detail, `RDDFromFiles(argv + 1, argc - 1)` will construct a first 
RDD having partitions with number of files passed to the argument. 
Then `map(files, GetLines)` will construct a second RDD with `Getlines`
as a function to be applied. Until now, no actual transformations are 
applied. Only a blueprint of transformations is made. 

After `count` is called, the transformations are applied. We use the 
term **materialize** for applying the transformation. Then for each 
partitions, `count` will iterate all the entries.

Here are the steps of applying transformation. 
1. The first RDD which is created by `RDDFromFiles` will call its 
iterator. For file RDDs, iterator will only return `*fp` as described 
in Code Overview below.
2. The second RDD which is created by `map` will call its iterator. 
Iterator will apply `fn` supplied for each elements of dependent RDD. 
In this example, it will be the First RDD, and it will apply `fn` until 
`fn` returns NULL. 
    
    * `map` transform will apply `fn` for each elements in dependent RDD
    * In this case, the first RDD is file backed, so it will return `fp`
    whenever it requires to iterate. 
    * Therefore, this transform will call `Getline()` to `fp` until it 
    returns NULL, which will be EOF.
3. `count` will iterate all the partitions of the second RDD. For each 
partitions, it will iterate all the elements in the partition. It will 
count the number of iterations made, and return it as the result of
`count`. 


## Thinking about parallelism
RDDs form a directed acyclic graph (DAG) and data within each RDD is
partitioned. Thus, there are two big opportunities for parallelism in
this project:
- Partitions of the same RDD may be materialized in parallel.
- Portions of the DAG which do not depend on each other may be
  materialized in parallel.

We consider a RDD1 to depend on RDD2 if the partitions of RDD2 must be
materialized before RDD1 can be materialized. Initially, the only RDDs
which have no dependencies are those which load data from files. These
RDDs form the "leaves" of the DAG. A good strategy is to recursively
traverse the DAG (remember, each RDD has 0, 1, or 2 dependencies, and
there are no cycles) and then materialize RDDs, working backwards
until reaching the top of the DAG. Be careful not to flood the work
queue with tasks that need to wait for their dependents to
finish. This is likely to result in deadlock!

### More detail and a potential optimization
The original Spark paper introduced the idea of "narrow" and "wide"
dependencies between RDDs. This optimization is **not required** for the
project and you can consider this as an extra optimization that can 
be made for this project. 

#### Narrow dependencies
Partitions have a narrow dependency if they have only single
dependency on another partition. `map` and `filter` are two examples
of transformations with narrow dependencies. Narrow dependencies make
parallelism easy because no data must be shared between subsequent
partitions.

#### Wide dependencies
Partitions have a wide dependency if they depend on more than one
partition. `partitionBy` and `join` both have wide dependencies
because multiple partitions must be read to satisfy each
operation.

#### Optimization
Instead of materializing RDDs one at a time as their dependent task(s)
complete, you can skip materializing intermediate RDDs in a chain of
narrow dependencies entirely. Consider a chain of `map`
operations. Each `map` consumes and produces exactly one value within
the same partition. Thus, the transformations can be "chained" without
materializing every intermediate result. By contrast, transformations
with wide dependencies have multiple dependent partitions. Each
dependent must be materializing prior to executing the wide
dependency. This optimization isn't required to pass any of our tests.

## A more complicated DAG
![ComplicatedExample](graphics/join.png)

This DAG has a mix of narrow and wide dependencies. Since RDD3 is a
partitioner, RDD2 must be materialized before RDD3. The same logic
applies to RDD6 and RDD7. RDD2 and RDD6 could be materialized
concurrently since they are in separate components of the DAG. Lastly,
RDD3 and RDD7 must be materialized before the join in RDD8 is
materialized.

## Code Overview
Here we describe all the code included with the project:
- `applications/`: example applications using MiniSpark are stored
  here. The `Makefile` included with the project will compile all of
  the applications and put executables in `bin/`. These are intended
  as short test programs to demonstrate the capabilities of MiniSpark
  and help you debug your solutions. Most of the tests are similar to
  the example applications.
- `lib/`: `lib.c` and `lib.h` define all the functions used by
  MiniSpark applications and tests. `lib.h` is commented with their
  descriptions. Nothing in this directory should be modified.
- `tests/`: all the tests for the project. Use `run-tests.sh` to run
  the tests.

Lastly, there is some code in the `solution/` directory. Here is where
you will writing MiniSpark. We have provided `minispark.h`, the header
file which defines the interface for MiniSpark, and `minispark.c`,
which has some code to get you started.
- `minispark.h`: this file holds all the public types and function
  prototypes for applications to use MiniSpark. Applications (like
  those in `applications/` and `tests`) include `minispark.h` to use
  MiniSpark. Thus, you should not modify any of the types or functions
  in this file. The header is well-commented with descriptions of all
  the types and functions.
- `minispark.c`: this is where you will write your solution. We have
  provided all of the RDD constructors in this file -- our code will
  build the DAG for you. 
	
Read all about the function and transformation types in
`minispark.h`. Here is a brief description of each transformation and
the arguments taken by each transformation function. Each
transformation takes one or two RDDs as input, and produces a new RDD
by applying the transform function to each element in the input
RDD(s). Some transformations use an additional `ctx` argument, but the
MiniSpark framework is oblivious to its contents. Minispark merely
needs to ensure `ctx` is passed to the transform
function. Constructors for all these are provided, but we do not
provide code for applying the transformations.

Commonly this `ctx`, which is the abbreviation of context, is used to 
pass the required arguments to the function. Examples from the 
applications folder might be useful to know what `ctx` is for. 

- `map(RDD* rdd, Mapper fn)`: produce an RDD where `fn` is applied to
  each element in each partition of `rdd`.
- `filter(RDD* rdd, Filter fn, void* ctx)`: produce an RDD where `fn`
  filters each element in each partition of `rdd`. If `fn` returns
  non-zero, the element should be added to the corresponding partition
  in the output RDD, otherwise it should not be.
- `join(RDD* rdd1, RDD* rdd2, Joiner fn, void* ctx)`: produce an RDD
  in which elements of `rdd1` and `rdd2` with the same key are joined
  according to `fn`. We make several simplifying assumptions for
  `join`.
  - input RDDs are each hash-partitioned with the same partitioner and
    number of partitions.
  - We will only perform inner joins. That is, `join` only returns a
    value when the key is in each input RDD.
  - There won't be duplicate keys in the input RDDs.
  - Input RDDs are not necessarily sorted. Therefore, you can do an
    inner join in O(n^2) time (i.e., two `for` loops).
- `partitionBy(RDD* rdd, Partitioner fn, int numpartitions, void*
  ctx)`: produce an RDD which is a hash-partitioned version of the
  input `rdd`. The number of output partitions is determined by
  `numpartitions`.
- `RDDFromFiles(char* filenames[], int numfiles)`: a special RDD
  constructor which we use to read from files. The output RDD has one
  `FILE*` per partition, no dependencies, and an `identity` mapper
  which simply returns the `FILE*` whenever a partition is iterated.
  There will not be any case that a RDD from `RDDFromFiles` will be 
  a root RDD, which might iterate forever. 

### Aside: understanding Join and PartitionBy
Although you won't have to implement joiners or partitioners, we
provide here a bit more detail to clarify our assumptions for
join. Join and partition are operations on key-value data. Generally,
we produce key-value data by splitting lines of text by whitespace --
any column in the line can be considered a key. Our join functions
return the result of the join if both input keys match, or NULL if
not. Since we don't guarantee keys are sorted, it is necessary to
attempt N^2 pairwise comparisons, where N is the length of each input
partition, to compute the join. Here's an example of an inner join on
column 0 (the key). The join function is to sum the data (column 1).

```
Input:
RDD0          RDD1
x  1          a 2
a  5          c 3
b  6          b 4
z  10

Result:
RDD2
a  7
b  10
```

We guarantee our join only needs to consider two input partitions (one
from each input RDD), by hash-partitioning the data before a join if
there is more than one partition in each input RDD. Hash-partitioning
ensures data with the same key is in the same-numbered
partition. PartitionBy accepts data as an argument and returns a
partition number (starting with zero). Since PartitionBy repartitions
all data within an RDD, all input partitions to the partitioner must
be materialized prior to running PartitionBy. The order of data within
partitions following a PartitionBy transformation does not matter.

## Writing your solution
MiniSpark should run an action (`count` or `print`) by materializing
the RDD argument to the action in parallel and then running the action
on the materialized RDD. We've provided a little bit of template code
in `minispark.c` for each action:
- `int count(RDD* rdd)`: materialize `rdd` and
  return the number of elements in `rdd`.
- `void print(RDD* rdd, Printer p)`: materialize `rdd` and print each element in `rdd` with `Printer p`.
  
Every application or test program follows the same pattern. First,
they define a DAG of RDDs. Then, they launch MiniSpark with
`MS_Run`. With MiniSpark running, each application runs one or more
actions on the DAG. There will not be a scenario that performs two
different transform to a single RDD, so you do not need to care 
about the number of materialization made in a single RDD. Finally,
applications stop execution and free resources with `MS_TearDown`.

We won't ever reference the same RDD from multiple top-level RDDs in
the DAG. In other words, RDDs will never have multiple "in"
edges. This should simplify materialization and memory management a
little bit.

### `MS_Run()`
This function (declared in `minispark.h`) launches the MiniSpark
infrastructure. In particular, it should:
- Create a thread pool, work queue, and worker threads (more detail on
  this below).
- Create the task metric queue and start the metrics monitor thread
  (also, more detail below).

### `MS_TearDown()`
Also declared in `minispark.h`, this function tears down the MiniSpark
infrastructure.
- Destroy the thread pool.
- Wait for the metrics thread to finish and join it.
- Free any allocations (thread pools, queues, RDDs).

### Work queue and threads
#### Thread pool
Your MiniSpark implementation should use threads to compute the DAG in
parallel. How many threads should MiniSpark use? One simple strategy
is to use the same number of threads as cores available to the
process. You can determine this number with `sched_getaffinity()`.

```
cpu_set_t set;
CPU_ZERO(&set);

if (sched_getaffinity(0, sizeof(set), &set) == -1) {
  perror("sched_getaffinity");
  exit(1);
}

printf("number of cores available: %d\n", CPU_COUNT(&set));
```

MiniSpark should adapt to the number of available cores. When testing
your program, we will manipulate the number of available cores with
`taskset`. For workloads chosen to benefit from parallelism, we expect
higher performance with larger numbers of cores.

One extremely useful abstraction for managing threads is called a
_thread pool_. Thread pools are static allocations of threads which
execute tasks in a _work queue_. A thread pool consists of multiple
worker threads that will live until `thread_pool_destroy` is called. 
Worker threads will wait until the work is pushed to the work queue. 
If they found the work can be done, one of the worker thread will 
pop from the work queue and perform the work. After finishing work,
it will wait again until it finds another work to be done. 

Tasks are added to the queue by a
dispatcher or controller thread (often, the main thread of
execution). By using thread pools, we can avoid the high overhead of
creating and joining threads each time work needs to be done. We can
also effectively manage system resources by creating a fixed number of
threads. Although not graded directly (i.e., not part of the public
interface of MiniSpark) we recommend you create a thread pool to
manage threads in your MiniSpark project. A suggested interface for
this thread pool follows:
- `thread_pool_init(int numthreads)`: create the pool with
  `numthreads` threads. Do any necessary allocations.
- `thread_pool_destroy()`: `join` all the threads and deallocate any
  memory used by the pool.
- `thread_pool_wait()`: returns when the work queue is empty and all
  threads have finished their tasks. You can use it to wait until all 
  the tasks in the queue are finished. For example, you would not want
  to `count` before RDD is fully materialized. 
- `thread_pool_submit(Task* task)`: adds a task to the work queue.

The thread pool also needs to maintain some global state accessible to
all threads. Minimally, this can a thread-safe queue (similar to
producer-consumer), which you've learned from the lecture. If you are
not sure what it is, please refer conditional variable chapter of 
OSTEP. You may find it useful to keep track of additional
information like the amount of work in the queue, amount of work
in-progress, etc.

#### Worker threads
Once created, worker threads should wait for tasks to appear in the
work queue. This is a similar pattern to producer-consumer. We have
included a `Task` datatype in `minispark.h`. Each task materializes
the partition of a particular RDD. Keeping in mind that partitions can
only be materialized once their dependencies have been materialized,
tasks need to be executed in a particular order:
- A tempting but poor solution: threads choosing tasks with
  outstanding dependencies could wait on a condition variable until
  their dependencies are completed. Unfortunately, it is very easy to
  deadlock if all the threads are waiting for another task to
  complete.
- Recommended solution: add tasks to the work queue if it is known
  that their dependencies are completed. This strategy requires less
  synchronization, but you will need to keep track of how many
  dependencies for each RDD have been completed.
  
You will need synchronization (locks, CVs) to manage the work queue
bounded buffer, as well as for the main thread to wait for worker
threads to complete all their tasks at the end of an action.
  
### Lists
Our header file assumes you write some sort of List data structure. We
represent all the partitions in an RDD as a List, and each partition
is itself a List of pointers to data elements. Lists should be
dynamically allocated, and we recommend writing `init`, `append`,
`get`, `free`, and iteration (i.e., `next`, `seek_to_start`) functions
for your List.

### Metrics
Lastly, we will record some metrics about tasks in MiniSpark in a
logging file during the framework execution. Real systems record all
sorts of runtime metrics so that users can profile their
execution. For this project, we will record the time that a task is
created, the time a worker thread fetches it from the work queue, and
the duration of its execution. Metrics should be printed to a file
`metrics.log`. We will use a separate _monitoring thread_, which waits
for metrics to appear in a queue. Upon getting a metric from the
queue, the monitoring thread should print the information to a log
file. When worker threads are finished with a task, they should store
metrics for that task in the monitoring queue. Since there are many
worker threads, adding metrics to the queue will need to be done in a
thread-safe manner.

We've provided some template code for this part of the
project. `minispark.h` contains a `TaskMetric` type. The `TaskMetric`
holds several timestamps as well as the RDD pointer and partition
number of the task. `minispark.c` contains some hints for recording
timestamps using `clock_gettime()`, instructions for find the
difference between two timestamps using the (included) macro
`TIME_DIFF_MICROS`, and a logging function `print_formatted_metric`
which prints the metric to a file.

This portion of the project can be done later, once the bulk of
MiniSpark is working.

## Other Considerations
### Memory Management
Some of the functions used by MiniSpark applications allocate
memory. Since MiniSpark is oblivious to the data used by MiniSpark
applications, how do we know which memory must be freed by the
framework?

We will guarantee any _intermediate_ data will be freed by MiniSpark
functions in `lib.c`. Any materialized data in the final RDD should be
freed by the MiniSpark when it is done executing. We consider a few
examples:
- Mapper `void* GetLines(void* arg)`: this function reads from a file
  with `getline` and allocates memory for each line. It does not free
  the memory.
- Filter `int StringContains(void* arg, void* needle)`: this function
  returns 0 or 1 if `arg` contains `needle`. If `arg` is filtered out
  (i.e., `StringContains` returns 0), `StringContains` will also free
  `arg`.
- Mapper `void* SplitCols(void* arg)`: this function consumes a line
  of input (`arg`) and produces a `struct row`. Since `arg` is
  intermediate data, `SplitCols` is responsible for freeing `arg` and
  allocating the `struct row`.

Since Join is a quadratic operation in our implementation of
MiniSpark, we can't free any of the data in the Join until the
operation is complete. Therefore, we make the following policy for
Joiners -- _Join functions will always allocate new memory for the
result of a Join, and MiniSpark should free all data in the
materialized input partitions to the Join_.

### Using a thread sanitizer
You can use a thread sanitizer, which is provided by gcc by adding 
`-fsanitize=thread` flag.

Thread sanitizer is a tool like memory sanitizer we've used from 
previous projects. Rather than memory bug detection, thread sanitizer 
detects concurrecy bugs like race conditions and deadlocks. 

Bugs like race conditions often do not cause direct error. Program 
will not crash, and the race condition is often discovered after a lot
of lines are execueted. It is really hard to find, so this tool might
be useful. 

Also, using `assert` macro will be helpful to narrow down the scope.
The key of debugging is to narrow down the scope and making safe and
unsafe region. 

For general multithread debugging using gdb, please refer 
`gdb_concurrent_debugging.md` in the repo. (Credit to Dhruv)

### Notable Simplifications in the Project
These simplifications are already written in the project writeup, and
this is a summary of them. 
* There will be only one `count` or `print` in a single test. 
* A RDD will not be a dependency to multiple RDDs. In other word, a
RDD will have only one parent RDD.
* There are several simplifications for `join`. Please refer to it. 


### FAQ
* Q: In linecount example, `GetLine` does not return a set of lines.
  * A: It is an iterator. 
  Please refer to the linecount example explanation. 
* Q: RDD from `RDDFromFiles` have only one element. How it is iterated?
  * A: File backed RDD will return `fp` whenever it is iterated. 
* More questions will be added whenever I find some frequently 
asked questions.

### Grading
You should write your implementation of MiniSpark in `minispark.c`. We
will test your program (mostly) for correctness. A few tests will
evaluate performance, and MiniSpark will also be evaluated for memory
errors. Memory errors will be tested with only open testcases, so if you
pass them, you don't need to worry about memory errors. 

Also, hidden testcases will only consist of stressing with the large files
and complex RDDs. You do not need to worry about the corner cases if you 
pass all the open testcases. 


## Administrivia 
- **Due Date** by April 15, 2025 at 11:59 PM 
- Questions: We will be using Piazza for all questions.
- Collaboration: You may work with a partner for this project. If you
  do, you will also submit a partners.csv file with the cslogins of
  both individuals in your group when you turn in the project in the
  top-level directory of your submission (just like slipdays.). Make sure partners.csv is in a valid csv format. Copying
  code (from other groups) is considered cheating. [Read
  this](http://pages.cs.wisc.edu/~remzi/Classes/537/Spring2018/dontcheat.html)
  for more info on what is OK and what is not. Please help us all have
  a good semester by not doing this.
- This project is to be done on the [lab
  machines](https://csl.cs.wisc.edu/docs/csl/2012-08-16-instructional-facilities/),
  so you can learn more about programming in C on a typical UNIX-based
  platform (Linux).
- A few sample tests are provided in the project repository. To run
  them, execute `run-tests.sh` in the `tests/` directory. Try
  `run-tests.sh -h` to learn more about the testing script. Note these
  test cases are not complete, and you are encouraged to create more
  on your own.
- **Slip Days**: 
  - In case you need extra time on projects, you each will have 2 slip
    days for the first 3 projects and 2 more for the final
    three. After the due date we will make a copy of the handin
    directory for on time grading.
  - To use a slip days or turn in your assignment late you will submit
    your files with an additional file that contains **only a single
    digit number**, which is the number of days late your assignment
    is (e.g. 1, 2, 3). Each consecutive day we will make a copy of any
    directories which contain one of these `slipdays.txt` files.

  - `slipdays.txt` must be present at **the top-level directory** of
    your submission.
  - Example project directory structure. (some files are omitted)
  ```
  p5/
  ├─ solution/
  │  ├─ minispark.c
  │  ├─ minispark.h
  ├─ tests/
  ├─ ...
  ├─ slipdays.txt
  ├─ partners.csv
  ```
  - We will track your slip days and late submissions from project to
    project and begin to deduct percentages after you have used up
    your slip days.
  - After using up your slip days you can get up to 80% if turned in 1
    day late, 60% for 2 days late, and 40% for 3 days late, but for
    any single assignment we won't accept submissions after the third
    days without an exception. This means if you use both of your
    individual slip days on a single assignment you can only submit
    that assignment one additional day late for a total of 3 days late
    with a 20% deduction.
  - Any exception will need to be requested from the instructors.
  - Example of `slipdays.txt`:
```sh
$ cat slipdays.txt
1
```
## Submitting your work
- Run `submission.sh` 
- Download generated tar file
- Upload it to Canvas
  * Links to Canvas assignment (update): 
  * [Prof. Mike Swift's class](https://canvas.wisc.edu/courses/434150/assignments/2650864) 
  * [Prof. Ali Abedi's class](https://canvas.wisc.edu/courses/434155/assignments/2650866) 
