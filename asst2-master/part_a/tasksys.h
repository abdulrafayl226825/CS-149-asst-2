#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>
using namespace std;
/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
private:
  int number_of_threads;
  vector<thread *> thread_list;

public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const vector<TaskID> &deps);
  void threadWork(IRunnable *runnable, int num_total_tasks,
                  atomic<int> *counter);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
private:
  int number_of_threads;
  vector<thread> thread_pool;
  bool shutdown;
  mutex thread_mutex;
  IRunnable *current_runnable;
  int finished_tasks;
  int remaining_tasks;
  int total_tasks;

  void spinningThread();

public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
private:
  int number_of_threads;
  vector<thread> thread_pool;
  atomic<bool> shutdown;
  atomic<int> remaining_tasks;
  atomic<int> finished_tasks;
  atomic<int> total_tasks;
  atomic<int> task_counter;

  IRunnable *current_runnable;

  mutex thread_mutex;
  mutex sleep_mutex; // New mutex for sleeping threads
  condition_variable tasks_available_cv;
  condition_variable tasks_done_cv;

  void sleepingThread();

public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const vector<TaskID> &deps);
  void sync();
};

#endif
