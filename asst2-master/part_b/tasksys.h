#ifndef _TASKSYS_H
#define _TASKSYS_H
#include "atomic"
#include "itasksys.h"
#include "map"
#include "mutex"
#include "queue"
#include "set"
#include "thread"
#include "vector"
#include <condition_variable>
#include <cstddef>
#include <mutex>
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
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
//
// struct my_task {
//   int group_id;
//   IRunnable *runnable;
//   set<int> tasks_I_depends_upon;
//   set<int> tasks_that_depend_on_me;
//   int num_total_tasks;
//   my_task(int group_id, IRunnable *runnable, int num_total_tasks)
//       : group_id(group_id), runnable(runnable),
//         num_total_tasks(num_total_tasks) {}
//
//   my_task() : group_id(-1), runnable(nullptr), num_total_tasks(0) {}
// };
//
struct my_task {
  int group_id;
  IRunnable *runnable;
  set<int> tasks_I_depends_upon;
  set<int> tasks_that_depend_on_me;
  int num_total_subtasks;
  atomic<int> completed_subtasks;

  my_task(int group_id, IRunnable *runnable, int num_total_subtasks)
      : group_id(group_id), runnable(runnable),
        num_total_subtasks(num_total_subtasks), completed_subtasks(0) {}

  my_task()
      : group_id(-1), runnable(nullptr), num_total_subtasks(0),
        completed_subtasks(0) {}
};

struct subtask {
  my_task *parent_task;
  int subtask_id;
};

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
  vector<thread> thread_pool;
  int total_threads;

  map<int, my_task> allTasks;
  queue<subtask> ready_tasks;
  atomic<int> nextTaskID;
  int remaining_tasks;
  atomic<bool> shutDown;
  mutex thread_mutex;
  condition_variable available_check;
  condition_variable sync_condition;

public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void worker_thread();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const vector<TaskID> &deps);
  void sync();
};
#endif
