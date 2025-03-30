#include "tasksys.h"
#include <assert.h>
#include <atomic>
#include <cassert>
#include <iostream>
using namespace std;

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
    : ITaskSystem(num_threads), shutdown(false) {
  for (int i = 0; i < num_threads; i++)
    threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::worket_thread,
                         this);
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  // cout << "Destroyed..." << endl;
  shutdown = true;
  cv.notify_all();
  for (auto &i : threads) {
    i.join();
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  // cout << "Calling async" << endl;
  runAsyncWithDeps(runnable, num_total_tasks, {});
  // cout << "Calling end async" << endl;
  sync();
  // cout << "synicng ended" << endl;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {

  //
  // TODO: CS149 students will implement this method in Part B.
  //
  // cout << "runAsyncWithDeps got runnable ptr : " << runnable <<
  // std::endl;
  auto new_task =
      make_shared<WaitingGroup>(runnable, num_total_tasks, deps.size());

  unique_lock<mutex> lock(thread_mutex);
  waitingGroups[nextTaskID] = new_task;
  for (auto &i : deps) {
    if (marked.count(i)) {
      new_task->my_deps--;
    }
    assert(waitingGroups.find(i) != waitingGroups.end());
    waitingGroups[i]->depends_upon.insert(nextTaskID);
  }
  if (new_task->my_deps == 0) {
    ready_tasks.push(new_task);
    cv.notify_all();
  }
  // cout << "return runasync func" << endl;
  remaining_bulks.fetch_add(1, std::memory_order_acquire);
  return nextTaskID++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //
  while (true) {
  }
  return;
}
void TaskSystemParallelThreadPoolSleeping::worket_thread() {
  while (!shutdown.load()) {
    std::unique_lock<std::mutex> lock(thread_mutex);
    cv.wait(lock,
            [this] { return !ready_tasks.empty() || this->shutdown.load(); });
    // //cout << "worker thread woke up " << endl;
    if (shutdown.load()) {
      return;
    }
    assert(!ready_tasks.empty());
    auto task = ready_tasks.front();
    // //cout << "Got " << task->started_counts.load() << " / "
    // << task->num_total_tasks << endl;
    if (task->started_counts.load() == task->num_total_tasks) {
      ready_tasks.pop();
      continue;
    }
    lock.unlock();

    while (task->started_counts.load() != task->num_total_tasks) {

      auto taskToDo = task->started_counts.load();

      task->started_counts.fetch_add(1);
      task->runnable->runTask(taskToDo, task->num_total_tasks);
      task->completed_tasks.fetch_add(1);
      // cout << "At the end: " << task->completed_tasks.load() << " / "
      //<< task->num_total_tasks << endl;
      if (task->completed_tasks.load() == task->num_total_tasks) {
        // cout << "completed_tasks " << task->num_total_tasks << endl;

        // cout << "remaining_bulks are " << remaining_bulks.load() << endl;
        remaining_bulks.fetch_sub(1, std::memory_order_acq_rel);
        lock.lock();
        for (auto &i : task->depends_upon) {
          waitingGroups[i]->my_deps--;
          if (waitingGroups[i]->my_deps == 0) {
            ready_tasks.push(waitingGroups[i]);
            cv.notify_all();
          }
        }
        // cout << "Tasks remaining now: " << ready_tasks.size() << endl;
        lock.unlock();
      }
    }
  }
}
