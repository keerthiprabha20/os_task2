"""Dining Philosophers, N=4, monitor based.

Monitor abstraction = class with one lock + one Condition; all methods run under the lock.
Philosopher states: THINKING / HUNGRY / EATING. A philosopher picks up BOTH forks
atomically (inside the monitor) or not at all -> no hold-and-wait -> no deadlock.
STARVATION: the textbook version lets two neighbours alternate and starve the one between
them. Fix: each hungry philosopher gets a ticket; a philosopher may eat only if no
neighbour is EATING and no hungry neighbour holds an OLDER ticket. The oldest hungry
philosopher is therefore never overtaken, so waiting is bounded.
Run: python dining_monitor.py [--meals 3]     Output: dining_monitor_log.json
"""
import threading, time, random, json, argparse
from dining_semaphore import Sim, N

sim = Sim()
THINKING, HUNGRY, EATING = "THINKING", "HUNGRY", "EATING"

class DiningMonitor:
    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._state = [THINKING] * N; self._ticket = [None] * N; self._next = 0

    def _can_eat(self, i):
        for j in ((i - 1) % N, (i + 1) % N):
            if self._state[j] == EATING: return False
            if self._state[j] == HUNGRY and self._ticket[j] < self._ticket[i]: return False
        return True

    def pickup(self, i):
        with self._cond:
            self._state[i] = HUNGRY; self._ticket[i] = self._next; self._next += 1
            sim.emit(i, "hungry", state=HUNGRY)
            while not self._can_eat(i): self._cond.wait()
            self._state[i] = EATING
            sim.emit(i, f"pick forks {i},{(i+1)%N}", take=i)
            sim.emit(i, "eating", state=EATING, take=(i + 1) % N, meal=True)

    def putdown(self, i):
        with self._cond:
            self._state[i] = THINKING; self._ticket[i] = None
            sim.emit(i, f"put forks {i},{(i+1)%N}", drop=i)
            sim.emit(i, "done eating", state=THINKING, drop=(i + 1) % N)
            self._cond.notify_all()           # neighbours re-check their condition

mon = DiningMonitor()

def philosopher(i, meals):
    for _ in range(meals):
        time.sleep(random.uniform(0.05, 0.3)); sim.emit(i, "thinking")
        mon.pickup(i)
        time.sleep(random.uniform(0.1, 0.3))
        mon.putdown(i)

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--meals", type=int, default=3); a = p.parse_args()
    ts = [threading.Thread(target=philosopher, args=(i, a.meals)) for i in range(N)]
    [t.start() for t in ts]; [t.join() for t in ts]
    print("\nDone. meals per philosopher:", sim.meals)
    json.dump({"problem": "dining-philosophers", "approach": "monitor", "n": N,
               "events": sim.events}, open("dining_monitor_log.json", "w"), indent=1)
