"""Dining Philosophers, N=4, semaphore based.

DEADLOCK PREVENTION: a 'room' semaphore initialised to N-1 = 3. At most 3 philosophers
compete for forks at once, so at least one always has both neighbours' forks available
(breaks the circular-wait condition). Each fork is a binary semaphore.
STARVATION: CPython's Semaphore wakes waiters in FIFO order (in practice, not a language
guarantee), so no philosopher is overtaken forever on the room or on a fork. Every
philosopher also has a bounded meal count, so the run terminates.
Run: python dining_semaphore.py [--meals 3]     Output: dining_semaphore_log.json
"""
import threading, time, random, json, argparse

N = 4

class Sim:
    def __init__(self):
        self.lock = threading.Lock(); self.t0 = time.time(); self.events = []
        self.state = ["THINKING"] * N; self.fork_owner = [None] * N; self.meals = [0] * N
    def emit(self, who, action, state=None, take=None, drop=None, meal=False):
        with self.lock:
            if state: self.state[who] = state
            if take is not None: self.fork_owner[take] = who
            if drop is not None: self.fork_owner[drop] = None
            if meal: self.meals[who] += 1
            ev = {"seq": len(self.events), "t": round(time.time() - self.t0, 3), "actor": f"P{who}",
                  "action": action, "states": list(self.state), "fork_owner": list(self.fork_owner),
                  "meals": list(self.meals)}
            self.events.append(ev)
            print(f"{ev['t']:6.2f}s P{who} {action:18} {ev['states']} forks={ev['fork_owner']}")

sim = Sim()
room = threading.Semaphore(N - 1)
forks = [threading.Semaphore(1) for _ in range(N)]

def philosopher(i, meals):
    left, right = i, (i + 1) % N
    for _ in range(meals):
        time.sleep(random.uniform(0.05, 0.3)); sim.emit(i, "thinking")
        sim.emit(i, "hungry", state="HUNGRY")
        room.acquire()
        forks[left].acquire();  sim.emit(i, f"pick fork {left}", take=left)
        forks[right].acquire(); sim.emit(i, f"pick fork {right}", take=right)
        sim.emit(i, "eating", state="EATING", meal=True)
        time.sleep(random.uniform(0.1, 0.3))
        forks[left].release();  sim.emit(i, f"put fork {left}", drop=left)
        forks[right].release(); sim.emit(i, f"put fork {right}", drop=right)
        sim.emit(i, "done eating", state="THINKING")
        room.release()

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--meals", type=int, default=3); a = p.parse_args()
    ts = [threading.Thread(target=philosopher, args=(i, a.meals)) for i in range(N)]
    [t.start() for t in ts]; [t.join() for t in ts]
    print("\nDone. meals per philosopher:", sim.meals)
    json.dump({"problem": "dining-philosophers", "approach": "semaphore", "n": N,
               "events": sim.events}, open("dining_semaphore_log.json", "w"), indent=1)
