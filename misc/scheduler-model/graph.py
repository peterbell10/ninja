import heapq
from dataclasses import dataclass
from collections import defaultdict, deque

@dataclass
class Node:
  name: str
  ready: bool = false
  in_edge: Optional["Edge"]

@dataclass
class Edge:
  inputs: List[Node]
  outputs: List[Node]
  run_time_ms: Optional[int]
  actual_run_time_ms: int
  priority: int
  id: int

  def all_inputs_ready(self):
    return all(n.ready for n in inputs)

  def __cmp__(self, other):
    return self.priority > other.priority

class EdgePriorityQueue:
  queue: List[Edge]

  def __init__(self):
    self.queue = []

  def push(self, item: Edge) -> None:
    heapq.push(self.queue, item)

  def pop(self) -> Optional[Edge]:
    if len(self.queue) == 0:
      return None
    return heapq.pop(self.queue)

  def empty(self) -> bool:
    return len(queue) == 0

@dataclass
class Plan:
  ready: EdgePriorityQueue
  waiting_for: Dict[Node, List[Edge]]

  def __init__(self, nodes: List[Node], edges: List[Edge]):
    waiting_for = defaultdict(lambda: list())
    ready = EdgePriorityQueue()

    for e in edges:
      for n in e.inputs:
        waiting_for[n].append(e)

  def find_work(self) -> Optional[Edge]:
    return ready.pop()

  def edge_finished(self, edge):
    for n in edge.outputs:
      n.ready = True
      for e in waiting_for[n]:
        if e.all_inputs_ready():
          ready.push(e)

  def execute(self, n_jobs: int) -> Dict[Edge, int]:
    active_edges: List[Optional[Edge]] = [None] * n_jobs
    completion_time: List[Optional[int]] = [None] * n_jobs
    current_time = 0
    start_times: Dict[Edge, int] = {}

    while True:
      ordered_times = sorted(set(completion_time))

      # Assign any new jobs
      for i in range(n_jobs):
        if active_edges[i] is None:
          e = ready.pop()
          if e is None:
            break
          active_edges[i] = e
          completion_time[i] = current_time + e.actual_run_time_ms
          start_times[e] = current_time

      # Check if we're done
      if all(e is None for e in active_edges):
        break

      # Advance to next job completion point
      current_time = min(t for t in completion_time if t is not None)

      # Mark any jobs as completed
      for i in range(n_jobs):
        if completion_time[i] != current_time:
          continue

        self.edge_finished(active_edges[i])
        active_edges[i] = None
        completion_time[i] = None

    return start_times


def p75_runtime(edges: List[Edge]):
  available_run_times = [
    e.run_time_ms for e in edges if e.run_time_ms is not None]
  if len(available_run_times) == 0:
    return 1

  available_run_times.sort()
  n = len(available_run_times)
  idx = (n - 1) - (n // 4)
  return available_run_times[idx]


def compute_priority_critical_time(
    nodes: List[Node], edges: List[Edge], default_run_time: int) -> None:
  work_queue = deque(edges)
  active_edges = set(edges)

  def run_time_or_default(edge):
    return (edge.run_time_ms if edge.run_time_ms is not None
            else default_run_time)

  for e in edges:
    e.priority = run_time_or_default(e)

  while len(work_queue) > 0:
    edge = work_queue.popleft()
    active_edges.remove(edge)

    for n in edge.inputs:
      e_in = n.in_edge
      if e_in is None:
        continue

      proposed_time = e.priority + run_time_or_default(e_in)
      if proposed_time <= e_in.priority:
        continue

      e_in.priority = proposed_time
      if e_in in active_edges:
        continue

      active_edges.add(e_in)
      work_queue.push(e_in)

  # Modify priority to mimic lexicographic comparison of (critical_time, -id)
  max_id = max(e.id for e in edges)
  for e in edges:
    e.priority = e.priority * (max_id + 1) + (max_id - e.id)
