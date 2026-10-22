"""Readers-Writers (monitor based, FAIR / starvation-free).

Python has no native monitor, so the monitor abstraction = one class whose
methods all run under ONE lock, with a Condition variable bound to that lock.
  - Private state: readers, writing, next_ticket, now_serving
  - Public entry points: start_read / end_read / start_write / end_write
FIFO tickets give fairness: a request may only enter when it is at the head of
the queue. Readers at the head enter together (each bumps now_serving); a writer
at the head waits for active readers to drain, blocking later readers meanwhile.
Run: python rw_monitor.py [--readers 4] [--writers 2] [--rounds 3]
Output: rw_monitor_log.json
"""
import threading, time, random, json, argparse
from rw_semaphore import Sim, Shared   # reuse logger + shared data class

sim, data = Sim(), Shared()

class RWMonitor:
    def __init__(self):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._readers = 0; self._writing = False
        self._next_ticket = 0; self._now_serving = 0

    def start_read(self, who):
        with self._cond:
            t = self._next_ticket; self._next_ticket += 1
            sim.emit(who, "request", waiting_readers=1)
            while not (t == self._now_serving and not self._writing):
                self._cond.wait()
            self._readers += 1; self._now_serving += 1
            sim.emit(who, "start_read", waiting_readers=-1, active_readers=1)
            self._cond.notify_all()          # next reader in line may enter too

    def end_read(self, who):
        with self._cond:
            self._readers -= 1
            sim.emit(who, "end_read", active_readers=-1)
            if self._readers == 0: self._cond.notify_all()

    def start_write(self, who):
        with self._cond:
            t = self._next_ticket; self._next_ticket += 1
            sim.emit(who, "request", waiting_writers=1)
            while not (t == self._now_serving and not self._writing and self._readers == 0):
                self._cond.wait()
            self._writing = True
            sim.emit(who, "start_write", waiting_writers=-1, active_writers=1)

    def end_write(self, who):
        with self._cond:
            self._writing = False; self._now_serving += 1
            sim.emit(who, "end_write", active_writers=-1)
            self._cond.notify_all()

mon, violations = RWMonitor(), 0

def reader(name, rounds):
    global violations
    for _ in range(rounds):
        time.sleep(random.uniform(0.05, 0.4))
        mon.start_read(name)
        x = data.a; time.sleep(random.uniform(0.1, 0.3)); y = data.b
        if x != y: violations += 1; sim.emit(name, "VIOLATION")
        mon.end_read(name)

def writer(name, rounds):
    for _ in range(rounds):
        time.sleep(random.uniform(0.1, 0.5))
        mon.start_write(name)
        data.a += 1; time.sleep(random.uniform(0.1, 0.3)); data.b += 1
        mon.end_write(name)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--readers", type=int, default=4); p.add_argument("--writers", type=int, default=2)
    p.add_argument("--rounds", type=int, default=3); a = p.parse_args()
    ts = [threading.Thread(target=reader, args=(f"R{i+1}", a.rounds)) for i in range(a.readers)]
    ts += [threading.Thread(target=writer, args=(f"W{i+1}", a.rounds)) for i in range(a.writers)]
    [t.start() for t in ts]; [t.join() for t in ts]
    print(f"\nDone. final value={data.a}, consistency violations={violations}")
    json.dump({"problem": "readers-writers", "approach": "monitor", "readers": a.readers,
               "writers": a.writers, "violations": violations, "events": sim.events},
              open("rw_monitor_log.json", "w"), indent=1)
