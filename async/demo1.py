import queue
import threading


q = queue.Queue(maxsize=5)
try:
    for i in range(5):
        q.put(i, block=False)
except queue.Full:
    print("Queue is Full with 3 items.")
try:
    for i in range(5):
        print(f"element {q.get(block=False)}")
except queue.Empty:
    print("Queue is already empty")
    
    
q = queue.PriorityQueue()

for i in [4,1,3,2,0]:
    q.put(i)
while not q.empty():
    print(q.get())
# result: 0,1,2,3,4