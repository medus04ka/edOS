#include "schedy.h"

#include <assert.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "coroed/api/task.h"
#include "coroed/core/relax.h"
#include "coroed/core/spinlock.h"
#include "kthread.h"
#include "uthread.h"

enum {
  /**
   * Максимальное количество файберов, которое может
   * одновременно выполняться на планировщике.
   */
  SCHED_THREADS_LIMIT = 512,

  /**
   * Количество рабочих потоков для исполнения файберов.
   */
  SCHED_WORKERS_COUNT = (size_t)(8),

  /**
   * Обеспечивает костыль для ретрая операций
   * `submit` и `acquire_next` при высокой
   * конкуренции на спинлоках файберов.
   */
  SCHED_NEXT_MAX_ATTEMPTS = (size_t)(16),
};

/**
 * Задача, выполняющаяся на планировщике.
 *
 * Hint: также тут можно хранить список задач, ожидающих
 *       завершения этой, чтобы уведомить их о данном
 *       событии.
 */
struct task {
  /**
   * Контекст исполнения.
   */
  struct uthread* thread;

  /**
   * Установлен при `UTHREAD_RUNNING`. Указывает на
   * рабочий поток, на котором исполняется задача.
   * Необходим для получения контекста локального
   * планировщика при операциях `yield`, `await`, `submit`.
   */
  struct worker* worker;

  enum {
    /** Готова к исполнению. */
    UTHREAD_RUNNABLE,

    /** Прямо сейчас выполняется. */
    UTHREAD_RUNNING,
    /** Заблокирована, не может выполняться. */
    UTHREAD_BLOCKED,

    /** Завершена и скоро станет зомби. */
    UTHREAD_FINISHED,

    /** Отработала и может быть переиспользована. */
    UTHREAD_ZOMBIE,
  } state;  // Текущее состояние задачи

  /**
   * Защищает поля структуры от неупорядоченного доступа.
   */
  struct spinlock lock;
  uint64_t last_state_change_ns;
  uint64_t time_running_ns;
  uint64_t time_runnable_ns;
  uint64_t time_blocked_ns;
};

/**
 * Рабочий поток, исполняющий файберы.
 */
struct worker {
  /**
   * Идентификатор рабочего потока.
   */
  size_t index;

  /**
   * Поток операционной системы, на котором
   * исполняется рабочий.
   */
  struct kthread kthread;

  /**
   * Контекст планировщика. Нужно переключиться на него,
   * чтобы вернуться в планировщик.
   */
  struct uthread sched_thread;

  /**
   * В данный момент исполняемая на рабочем
   * задача. Может быть `NULL`.
   */
  struct task* running_task;

  struct {
    size_t steps;     // Сколько шагов было выполнено
    size_t finished;  // Сколько задач было завершено
  } statistics;       // Локальная статистика работяги
};

static struct spinlock tasks_lock;              // Защищает список задач
static size_t next_task_index = 0;              // Для планирования round-robin
static struct task tasks[SCHED_THREADS_LIMIT];  // Список всех задач

// Hint: для реализации более сложных схем управления, вам
//       вам могут понадобиться связаные списки (`core/list.h`).

static kthread_id_t kthread_ids[SCHED_WORKERS_COUNT];
static struct worker workers[SCHED_WORKERS_COUNT];
struct scheduler_metrics {
  size_t n_running;
  size_t n_runnable;
  size_t n_blocked;

  uint64_t total_running_ns;
  uint64_t total_runnable_ns;
  uint64_t total_blocked_ns;

  size_t finished_count;

  uint64_t create_times_ns[SCHED_THREADS_LIMIT];
  size_t create_count;
};

static struct scheduler_metrics g_metrics;

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void metrics_on_state_change(struct task* task, int new_state) {
  uint64_t t = now_ns();

  if (task->last_state_change_ns != 0) {
    uint64_t delta = t - task->last_state_change_ns;

    switch (task->state) {
      case UTHREAD_RUNNING:
        task->time_running_ns += delta;
        g_metrics.total_running_ns += delta;
        if (g_metrics.n_running > 0) {
          g_metrics.n_running--;
        }
        break;
      case UTHREAD_RUNNABLE:
        task->time_runnable_ns += delta;
        g_metrics.total_runnable_ns += delta;
        if (g_metrics.n_runnable > 0) {
          g_metrics.n_runnable--;
        }
        break;
      case UTHREAD_BLOCKED:
        task->time_blocked_ns += delta;
        g_metrics.total_blocked_ns += delta;
        if (g_metrics.n_blocked > 0) {
          g_metrics.n_blocked--;
        }
        break;
      default:
        break;
    }
  }

  task->last_state_change_ns = t;

  switch (new_state) {
    case UTHREAD_RUNNING:
      g_metrics.n_running++;
      break;
    case UTHREAD_RUNNABLE:
      g_metrics.n_runnable++;
      break;
    case UTHREAD_BLOCKED:
      g_metrics.n_blocked++;
      break;
    default:
      break;
  }

  task->state = new_state;
}

static void metrics_on_task_created(struct task* task) {
  task->last_state_change_ns = 0;
  task->time_running_ns = 0;
  task->time_runnable_ns = 0;
  task->time_blocked_ns = 0;

  g_metrics.create_times_ns[g_metrics.create_count++] = now_ns();

  metrics_on_state_change(task, UTHREAD_RUNNABLE);
}

/**
 * Установить задачу в пустое состояние.
 */
void sched_task_init(struct task* task) {
  task->thread = NULL;
  task->worker = NULL;
  task->state = UTHREAD_ZOMBIE;
  task->last_state_change_ns = 0;
  task->time_running_ns = 0;
  task->time_runnable_ns = 0;
  task->time_blocked_ns = 0;
  spinlock_init(&task->lock);
}

/**
 * Установить рабочего в пустое состояние.
 */
void sched_worker_init(struct worker* worker, size_t index) {
  worker->index = index;
  worker->sched_thread.context = NULL;
  worker->running_task = NULL;
  worker->statistics.steps = 0;
  worker->statistics.finished = 0;
}

void sched_init() {
  spinlock_init(&tasks_lock);
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    sched_task_init(&tasks[i]);
  }
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    kthread_ids[i] = 0;
    sched_worker_init(&workers[i], i);
  }
  memset(&g_metrics, 0, sizeof(g_metrics));
}

/**
 * Находясь в контексте задачи `task`, переключиться
 * в контекст планировщика.
 */
void sched_switch_to_scheduler(struct task* task) {
  struct uthread* sched = &task->worker->sched_thread;
  task->worker = NULL;
  uthread_switch(task->thread, sched);
}

/**
 * Находясь в контексте планировщика, переключиться
 * в контекст задачи `task`. Выполнять ее до следующего
 * невынужденного возвращения в планировщик.
 */
void sched_switch_to(struct worker* worker, struct task* task) {
  assert(task->thread != &worker->sched_thread);

  metrics_on_state_change(task, UTHREAD_RUNNING);

  task->worker = worker;
  worker->running_task = task;

  struct uthread* sched = &worker->sched_thread;
  uthread_switch(sched, task->thread);
}

/**
 * Получить следующую задачу на исполнение в
 * соответствии с текущим алгоритмом планирования.
 *
 * Вызывающему передается владение задачей, а также
 * захваченный лок `task->lock`.
 */
struct task* sched_acquire_next();

/**
 * Вернуть задачу в очередь планирования.
 *
 * Очереди передается владение задачей,
 * а также она отпустит лок `task->lock`.
 */
void sched_release(struct task* task);

/**
 * Цикл планировщика. Выполняется, пока есть задачи.
 */
int sched_loop(void* argument) {
  struct worker* worker = argument;
  kthread_ids[worker->index] = kthread_id();

  for (;;) {
    struct task* task = sched_acquire_next();
    if (task == NULL) {
      break;
    }

    sched_switch_to(worker, task);

    worker->statistics.steps += 1;
    if (task->state == UTHREAD_FINISHED) {
      worker->statistics.finished += 1;
    }

    sched_release(task);

    // Hint: где-то здесь можно было бы опросить
    //       механизмы для неблокирующего ввода-вывода
    //       и перевести удовлетворенные BLOCKED
    //       потоки в RUNNABLE состояние.
  }

  return 0;
}

struct task* sched_acquire_next() {
  // На всякий случай пытаемся найти задачу несколько раз,
  // так как какие-то `task->lock` могли быть отпущены.

  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    spinlock_lock(&tasks_lock);  // Защитим `next_task_index`

    for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
      struct task* task = &tasks[next_task_index];
      if (!spinlock_try_lock(&task->lock)) {
        continue;
      }

      // Планирование round-robin
      next_task_index = (next_task_index + 1) % SCHED_THREADS_LIMIT;

      if (task->thread != NULL && task->state == UTHREAD_RUNNABLE) {
        spinlock_unlock(&tasks_lock);
        return task;
      }

      spinlock_unlock(&task->lock);
    }

    spinlock_unlock(&tasks_lock);
    SPINLOOP(2 * attempt);
  }

  return NULL;
}

void sched_release(struct task* task) {
  task->worker = NULL;
  if (task->state == UTHREAD_FINISHED) {
    // Состояние RUNNING -> FINISHED уже обработано в sched_finish
    // с точки зрения метрик. Здесь просто делаем ZOMBIE.
    uthread_reset(task->thread);
    task->state = UTHREAD_ZOMBIE;
  } else if (task->state == UTHREAD_RUNNING) {
    metrics_on_state_change(task, UTHREAD_RUNNABLE);
  } else if (task->state == UTHREAD_BLOCKED) {
    // Ничего не делаем, BLOCKED состояние уже уybxчетено.
  }
  spinlock_unlock(&task->lock);
}

/**
 * Отметить задачу завершенной.
 */
void sched_finish(struct task* task) {
  metrics_on_state_change(task, UTHREAD_FINISHED);
  g_metrics.finished_count++;
}

void task_yield(struct task* caller) {
  sched_switch_to_scheduler(caller);
}

void task_exit(struct task* caller) {
  sched_finish(caller);
  task_yield(caller);
}

void task_block(struct task* caller) {
  metrics_on_state_change(caller, UTHREAD_BLOCKED);
  sched_switch_to_scheduler(caller);
}

void task_unblock(struct task* task) {
  spinlock_lock(&task->lock);
  if (task->state == UTHREAD_BLOCKED) {
    metrics_on_state_change(task, UTHREAD_RUNNABLE);
  }
  spinlock_unlock(&task->lock);
}

task_t task_submit(struct task* caller, uthread_routine entry, void* argument) {
  (void)caller;  // Может быть полезно.
  task_t child = sched_submit(*entry, argument);
  return child;
}

task_t sched_try_submit(void (*entry)(), void* argument) {
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];

    if (!spinlock_try_lock(&task->lock)) {
      continue;
    }

    if (task->thread == NULL) {
      task->thread = uthread_allocate();
      assert(task->thread != NULL);
      task->state = UTHREAD_ZOMBIE;
    }

    const bool is_submitted = task->state == UTHREAD_ZOMBIE;

    if (task->state == UTHREAD_ZOMBIE) {
      uthread_reset(task->thread);
      uthread_set_entry(task->thread, entry);
      uthread_set_arg_0(task->thread, task);
      uthread_set_arg_1(task->thread, argument);
      metrics_on_task_created(task);
    }

    spinlock_unlock(&task->lock);
    if (is_submitted) {
      return (task_t){.task = task};
    }
  }

  return (task_t){.task = NULL};
}

task_t sched_submit(void (*entry)(), void* argument) {
  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    task_t handle = sched_try_submit(entry, argument);
    if (handle.task != NULL) {
      return handle;
    }
    SPINLOOP(2 * attempt);
  }

  assert(false && "Can't create a task");
}

void sched_start() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_create(&worker->kthread, sched_loop, worker);
    assert(status == KTHREAD_SUCCESS);
  }
}

void sched_wait() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_join(&worker->kthread);
    assert(status == KTHREAD_SUCCESS);
  }
}

void sched_print_statistics() {
  printf("\nsched statistics\n");

  size_t tasks_count = 0;
  size_t steps_count = 0;
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    tasks_count += worker->statistics.finished;
    steps_count += worker->statistics.steps;
  }

  printf("|- tasks executed %zu\n", tasks_count);
  printf("|- steps done     %zu\n", steps_count);

  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    printf("|- worker %zu %zu\n", i, kthread_ids[i]);
    printf("   |- steps     %zu\n", worker->statistics.steps);
    printf("   |- finished  %zu\n", worker->statistics.finished);
  }

  printf("\nmetrics (scheduler)\n");
  printf("|- running   count %zu\n", g_metrics.n_running);
  printf("|- runnable  count %zu\n", g_metrics.n_runnable);
  printf("|- blocked   count %zu\n", g_metrics.n_blocked);
  printf("|- finished  count %zu\n", g_metrics.finished_count);
  printf("|- total running ns  %lu\n", (unsigned long)g_metrics.total_running_ns);
  printf("|- total runnable ns %lu\n", (unsigned long)g_metrics.total_runnable_ns);
  printf("|- total blocked ns  %lu\n", (unsigned long)g_metrics.total_blocked_ns);
}

void sched_destroy() {
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];
    spinlock_lock(&task->lock);
    if (task->thread != NULL) {
      uthread_free(task->thread);
    }
    spinlock_unlock(&task->lock);
  }
}
