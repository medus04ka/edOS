#include "event.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "task.h"

void event_init(struct event* event) {
  atomic_store(&event->is_fired, false);
  event->waiters_count = 0;
}

void event_wait(struct task* caller, struct event* event) {
  if (atomic_load(&event->is_fired)) {
    return;
  }

  size_t index = event->waiters_count;
  assert(index < EVENT_MAX_WAITERS);
  event->waiters[index] = caller;
  event->waiters_count = index + 1;

  task_block(caller);
}

void event_fire(struct event* event) {
  atomic_store(&event->is_fired, true);
  for (size_t i = 0; i < event->waiters_count; ++i) {
    struct task* waiter = event->waiters[i];
    if (waiter != NULL) {
      task_unblock(waiter);
    }
  }
}