"""Readers-Writers (semaphore based, FAIR / starvation-free).

Semaphores
  service : FIFO turnstile. Every reader/writer passes through it in arrival order,
            so a stream of readers cannot starve a writer (and vice versa).
  resource: held by the writer, or collectively by the group of active readers.
  rmutex  : protects read_count.
Run: python rw_semaphore.py [--readers 4] [--writers 2] [--rounds 3]
Output: rw_semaphore_log.json  (events for the HTML simulation)
"""
import threading, time, random, json, argparse

class Sim:
    def __init__(self):
        self.lock = threading.Lock(); self.t0 = time.time(); self.events = []
        self.st = dict(waiting_readers=0, waiting_writers=0, active_readers=0, active_writers=0)
    def emit(self, actor, action, **delta):
        with self.lock:
            for k, v in delta.items(): self.st[k] += v
            ev = {"seq": len(self.events), "t": round(time.time() - self.t0, 3),
                  "actor": actor, "action": action, **self.st}
            self.events.append(ev)
            print(f"{ev['t']:6.2f}s {actor:4} {action:12} R={ev['active_readers']} W={ev['active_writers']} "
                  f"waitR={ev['waiting_readers']} waitW={ev['waiting_writers']}")

class Shared:                      # two fields that must always be equal
    def __init__(self): self.a = self.b = 0

sim, data = Sim(), Shared()
service, resource, rmutex = threading.Semaphore(1), threading.Semaphore(1), threading.Semaphore(1)
read_count, violations = 0, 0

def reader(name, rounds):
    global read_count, violations
    for _ in range(rounds):
        time.sleep(random.uniform(0.05, 0.4))
        sim.emit(name, "request", waiting_readers=1)
        service.acquire()                      # wait in line
        rmutex.acquire()
        read_count += 1
        if read_count == 1: resource.acquire() # first reader locks writers out
        service.release()
        rmutex.release()
        sim.emit(name, "start_read", waiting_readers=-1, active_readers=1)
        x = data.a; time.sleep(random.uniform(0.1, 0.3)); y = data.b
        if x != y: violations += 1; sim.emit(name, "VIOLATION")
        sim.emit(name, "end_read", active_readers=-1)
        rmutex.acquire()
        read_count -= 1
        if read_count == 0: resource.release() # last reader lets writers in
        rmutex.release()

def writer(name, rounds):
    for _ in range(rounds):
        time.sleep(random.uniform(0.1, 0.5))
        sim.emit(name, "request", waiting_writers=1)
        service.acquire()
        resource.acquire()                     # exclusive access
        service.release()
        sim.emit(name, "start_write", waiting_writers=-1, active_writers=1)
        data.a += 1; time.sleep(random.uniform(0.1, 0.3)); data.b += 1
        sim.emit(name, "end_write", active_writers=-1)
        resource.release()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--readers", type=int, default=4); p.add_argument("--writers", type=int, default=2)
    p.add_argument("--rounds", type=int, default=3); a = p.parse_args()
    ts = [threading.Thread(target=reader, args=(f"R{i+1}", a.rounds)) for i in range(a.readers)]
    ts += [threading.Thread(target=writer, args=(f"W{i+1}", a.rounds)) for i in range(a.writers)]
    [t.start() for t in ts]; [t.join() for t in ts]
    print(f"\nDone. final value={data.a}, consistency violations={violations}")
    json.dump({"problem": "readers-writers", "approach": "semaphore", "readers": a.readers,
               "writers": a.writers, "violations": violations, "events": sim.events},
              open("rw_semaphore_log.json", "w"), indent=1)
