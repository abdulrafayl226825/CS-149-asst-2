#include "tasksys.h"
#include <thread>
#include <vector>
#include <atomic>
using namespace std;


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    number_of_threads = num_threads;
    thread_list.resize(number_of_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::threadWork(IRunnable* runnable, int num_total_tasks, atomic<int>* counter)
{
    while (true) 
    {
        int task_id = counter->fetch_add(1);

        if (task_id >= num_total_tasks)
        {
            return;
        }

        runnable->runTask(task_id, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) 
{
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

  /*   for (int i = 0; i < num_total_tasks; i++) 
    {        
        runnable->runTask(i, num_total_tasks);
    } */


    // Cannot Implement this Cyclic Distribution of Tasks as the work done by each thread is not uniform. In this distribution some threads will work more than others.
    // We need to assign tasks dynamically based on work instead of statically.
    /* vector<thread> threads;
    for (int thread_id = 0; thread_id < number_of_threads; thread_id++) 
    {
        threads.emplace_back([=]() 
        {
            for (int task_id = thread_id; task_id < num_total_tasks; task_id += number_of_threads) 
            {
                runnable->runTask(task_id, num_total_tasks);
            }
        });
    }

    // Wait for all threads to finish
    for (auto& t : threads) 
    {
        t.join();
    } */
    atomic<int>* counter = new atomic<int>(0);

    for (int i = 0; i < number_of_threads; i++) 
    {
        this->thread_list[i] = new thread(&TaskSystemParallelSpawn::threadWork, this, runnable, num_total_tasks, counter);
    }

    for (int i = 0; i < number_of_threads; i++)
    {
        this->thread_list[i]->join();
        delete this->thread_list[i];
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}


TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    number_of_threads = num_threads;
    shutdown = false;
    current_runnable = nullptr;
    finished_tasks = 0;
    remaining_tasks = 0;
    total_tasks = 0;

    for (int i = 0; i < number_of_threads; i++) 
    {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSpinning::spinningThread, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    thread_mutex.lock();
    shutdown = true;
    thread_mutex.unlock();

    for (auto& thread : thread_pool) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::spinningThread() 
{
    while (true) 
    {
        int task_id = -1;

        thread_mutex.lock();
        if (shutdown) 
        {
            thread_mutex.unlock();
            return;
        }

        if (remaining_tasks > 0) 
        {
            task_id = total_tasks - remaining_tasks;
            remaining_tasks--;
            thread_mutex.unlock();

            if (task_id >= 0 && task_id < total_tasks && current_runnable) 
            {
                current_runnable->runTask(task_id, total_tasks);
            }

            thread_mutex.lock();
            finished_tasks++;
            thread_mutex.unlock();
        } 
        else 
        {
            thread_mutex.unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) 
{
    thread_mutex.lock();
    finished_tasks = 0;
    remaining_tasks = num_total_tasks;
    total_tasks = num_total_tasks;
    current_runnable = runnable;
    thread_mutex.unlock();

    while (true) 
    {
        thread_mutex.lock();
        if (finished_tasks == total_tasks) 
        {
            thread_mutex.unlock();
            break;
        }
        thread_mutex.unlock();
    }
}


TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    number_of_threads = num_threads;
    shutdown.store(false);
    remaining_tasks.store(0);
    finished_tasks.store(0);
    total_tasks.store(0);
    task_counter.store(0);

    for (int i = 0; i < num_threads; i++) 
    {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() 
{
    shutdown.store(true);
    tasks_available_cv.notify_all();

    for (auto& thread : thread_pool) 
    {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::sleepingThread() 
{
    while (!shutdown.load()) 
    {
        int task_id = -1;

        unique_lock<mutex> lock(thread_mutex);
        tasks_available_cv.wait(lock, [this] { return remaining_tasks.load() > 0 || shutdown.load(); });

        if (shutdown.load()) 
        {
            return;
        }

        task_id = task_counter.fetch_add(1);

        if (task_id >= total_tasks.load()) 
        {
            continue;
        }
        
        remaining_tasks.fetch_sub(1);
        lock.unlock();

        if (current_runnable) 
        {
            current_runnable->runTask(task_id, total_tasks.load());
        }

        if (finished_tasks.fetch_add(1) + 1 == total_tasks.load()) 
        {
            lock_guard<mutex> done_lock(sleep_mutex);
            tasks_done_cv.notify_one();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) 
{
    unique_lock<mutex> lock(thread_mutex);

    finished_tasks.store(0);
    remaining_tasks.store(num_total_tasks);
    total_tasks.store(num_total_tasks);
    task_counter.store(0);
    current_runnable = runnable;

    lock.unlock();
    tasks_available_cv.notify_all();

    unique_lock<mutex> done_lock(sleep_mutex);
    tasks_done_cv.wait(done_lock, [this] { return finished_tasks.load() == total_tasks.load(); });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
