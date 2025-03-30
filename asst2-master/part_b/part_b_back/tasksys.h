#ifndef _TASKSYS_H
#define _TASKSYS_H
#include "atomic"
#include "itasksys.h"
#include "mutex"
#include "queue"
#include "thread"
#include "unordered_map"
#include "unordered_set"
#include "vector"
#include <condition_variable>
#include <cstddef>
#include <memory>
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
struct waitingGroup {
  int group_id;
  IRunnable *runnable;

  int num_total_subtasks;
  atomic<int> tasks_I_depends_upon;
  unordered_set<int> tasks_that_depend_on_me;
  atomic<int> completed_subtasks;

  waitingGroup(int group_id, IRunnable *runnable, int num_total_subtasks)
      : group_id(group_id), runnable(runnable),
        num_total_subtasks(num_total_subtasks), tasks_I_depends_upon(0),
        completed_subtasks(0) {}

  waitingGroup()
      : group_id(-1), runnable(nullptr), num_total_subtasks(0),
        completed_subtasks(0) {}
};

struct subtask {
  shared_ptr<waitingGroup> parent_task;
  int subtask_id;
  subtask(shared_ptr<waitingGroup> parent_task, int subtask_id)
      : parent_task(parent_task), subtask_id(subtask_id) {}
};

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
  vector<thread> thread_pool;
  queue<subtask> ready_tasks;
  atomic<int> nextTaskID;
  unordered_map<int, std::shared_ptr<waitingGroup>> waiting_tasks;
  atomic<int> remaining_tasks;
  atomic<bool> shutDown;
  mutex thread_mutex;
  unordered_set<TaskID> mark_completed;
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
