#pragma once

#include <stdatomic.h>
#include <stddef.h>

#include "coroed/api/task.h"

/**
 * Синтаксический сахар для использования в теле файбера.
 */
#define EVENT_WAIT(event) event_wait(__self, (event))

/**
 * Максимальное количество задач, которое может одновременно
 * ожидать одно событие
 */
enum { EVENT_MAX_WAITERS = 64 };

/**
 * Примитив синхронихации `Event`.
 *
 * Позволяет долждаться некоторого события
 * в текущем файбере. Событие может произойти
 * не более 1 раза.
 *
 * Пример использования ищите в `test_event.c`.
 */
struct event {
  atomic_bool is_fired;
  struct task* waiters[EVENT_MAX_WAITERS];
  size_t waiters_count;
};

/**
 * Конструктор объекта `Event`.
 *
 * Изначально событие не произошло (`is_fired = false`).
 */
void event_init(struct event* event);

/**
 * Дождаться события `event`, находясь в файбере `caller`.
 *
 * Возврат из функции произойдет только когда `is_fired = true`.
 * В текущей реализации файбер паркуется в состоянии BLOCKED
 */
void event_wait(struct task* caller, struct event* event);

/**
 * Отметить событие произошедшим и разблокировать всех ожидающих.
 */
void event_fire(struct event* event);