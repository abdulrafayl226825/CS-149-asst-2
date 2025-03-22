#include "tasksys.h"
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <stdexcept>

IRunnable::~IRunnable() {}
ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemSerial::sync() { return; }

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), nextTaskID(0), shutDown(false),
      total_threads(num_threads) {

  printf("Thread  in construction %d\n", num_threads);
  for (int i = 0; i < num_threads; i++) {

    thread_pool.emplace_back(
        &TaskSystemParallelThreadPoolSleeping::worker_thread, this);
  }
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}
//
// TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
// {
//   printf("Destructor war gya e\n");
//   shutDown.store(true);
//   available_check.notify_all();
//   for (auto &i : thread_pool)
//     if (i.joinable())
//       i.join();
//   //
//   // TODO: CS149 student implementations may decide to perform cleanup
//   // operations (such as thread pool shutdown construction) here.
//   // Implementations are free to add new class member variables
//   // (requiring changes to tasksys.h).
//   //
// }
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  {
    printf("destructing threads\n");
    lock_guard<mutex> lock(thread_mutex);
    shutDown.store(true);
  }
  available_check.notify_all(); // Wake up all threads

  for (auto &thread : thread_pool) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}
void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
}
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const vector<TaskID> &deps) {

  lock_guard<mutex> lock(thread_mutex);
  int new_id = nextTaskID++;

  // Create task and store it in allTasks
  // auto &new_task =
  //     allTasks.emplace(new_id, my_task(new_id, runnable, num_total_tasks))
  //         .first->second;
  auto result = allTasks.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(new_id), // Key: new_id (const int)
      std::forward_as_tuple(new_id, runnable,
                            num_total_tasks) // Value: my_task args
  );
  auto &new_task = result.first->second;
  // Add dependencies
  for (auto &i : deps) {
    auto it = allTasks.find(i);
    if (it == allTasks.end()) {
      throw std::runtime_error("Dependency task not found");
    }
    new_task.tasks_I_depends_upon.insert(i);
    it->second.tasks_that_depend_on_me.insert(new_id);
  }

  remaining_tasks++;

  // If the task has no dependencies, add all its subtasks to ready_tasks
  if (new_task.tasks_I_depends_upon.empty()) {
    for (int i = 0; i < new_task.num_total_subtasks; i++) {
      ready_tasks.push({&new_task, i}); // Enqueue subtasks
    }
    available_check.notify_all();
  }

  return new_id;
}
//
// TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
//     IRunnable *runnable, int num_total_tasks, const vector<TaskID> &deps) {
//
//   lock_guard<mutex> lock(thread_mutex);
//   int new_id = nextTaskID++;
//
//   // Create task with emplace to avoid default construction
//   auto &new_task =
//       allTasks.emplace(new_id, my_task(new_id, runnable, num_total_tasks))
//           .first->second;
//
//   for (auto &i : deps) {
//     // SAFE DEPENDENCY CHECK
//     auto it = allTasks.find(i);
//     if (it == allTasks.end()) {
//       throw std::runtime_error("Dependency task not found");
//     }
//
//     new_task.tasks_I_depends_upon.insert(i);
//     it->second.tasks_that_depend_on_me.insert(new_id);
//   }
//
//   remaining_tasks++;
//
//   if (new_task.tasks_I_depends_upon.empty()) {
//     ready_tasks.push(&new_task);
//     available_check.notify_one();
//   }
//
//   return new_id;
// }
void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  unique_lock<mutex> lock(thread_mutex);
  sync_condition.wait(lock, [this] { return remaining_tasks == 0; });
  return;
}
// void TaskSystemParallelThreadPoolSleeping::worker_thread() {
//   while (!shutDown.load()) {
//     unique_lock<mutex> lock(thread_mutex);
//     available_check.wait(
//         lock, [this] { return !ready_tasks.empty() || shutDown.load(); });
//
//     if (shutDown.load(std::memory_order_acquire)) {
//
//       return;
//     }
//     my_task *task = ready_tasks.front();
//     ready_tasks.pop();
//     lock.unlock();
//
//     // Execute the task without holding the lock
//
//     task->runnable->runTask(task->group_id, task->num_total_tasks);
//     {
//       lock_guard<mutex> lock(thread_mutex);
//       remaining_tasks--;
//       if (remaining_tasks == 0) {
//         sync_condition.notify_all(); // Notify sync() that all tasks are done
//       }
//     }
//
//     lock.lock();
//
//     for (auto &i : allTasks) {
//       auto &j = i.second;
//       printf("%d %d %d\n", i.second.group_id, j.group_id,
//              j.tasks_that_depend_on_me.size());
//       fflush(stdout);
//     }
//     for (auto &child_id : task->tasks_that_depend_on_me) {
//       auto it = allTasks.find(child_id);
//       if (it == allTasks.end()) {
//         // Log error or handle missing task
//         continue;
//       }
//       my_task &child = it->second;
//       child.tasks_I_depends_upon.erase(task->group_id);
//
//       if (child.tasks_I_depends_upon.empty()) {
//         ready_tasks.push(&child);
//         available_check.notify_one();
//       }
//     }
//
//     printf("print any possible issue\n");
//     fflush(stdout);
//     lock.unlock();
//   }
// }
void TaskSystemParallelThreadPoolSleeping::worker_thread() {
  while (true) {
    unique_lock<mutex> lock(thread_mutex);
    available_check.wait(
        lock, [this] { return !ready_tasks.empty() || shutDown.load(); });

    if (shutDown.load()) {
      return;
    }

    // Get the next subtask
    subtask st = ready_tasks.front();
    ready_tasks.pop();
    my_task *task = st.parent_task;
    int subtask_id = st.subtask_id;

    lock.unlock();

    // Execute the subtask
    task->runnable->runTask(subtask_id, task->num_total_subtasks);

    // Update completed subtasks
    int completed = ++(task->completed_subtasks);
    if (completed == task->num_total_subtasks) {
      // All subtasks done; process dependent tasks
      lock_guard<mutex> inner_lock(thread_mutex);
      remaining_tasks--;

      for (auto &child_id : task->tasks_that_depend_on_me) {
        auto it = allTasks.find(child_id);
        if (it == allTasks.end())
          continue;
        my_task &child = it->second;

        child.tasks_I_depends_upon.erase(task->group_id);

        if (child.tasks_I_depends_upon.empty()) {
          // Enqueue all subtasks of the child
          for (int i = 0; i < child.num_total_subtasks; i++) {
            ready_tasks.push({&child, i});
          }
          available_check.notify_one();
        }
      }

      if (remaining_tasks == 0) {
        sync_condition.notify_all(); // Unblock sync()
      }
    }
  }
}
