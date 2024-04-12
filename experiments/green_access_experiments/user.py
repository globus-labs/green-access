from abc import ABC, abstractmethod
import time
import json
from collections import defaultdict

from green_access import GreenAccessClient

class Strategy(ABC):
    @abstractmethod
    def __call__(self, predictions):
        pass

class MinCredit(Strategy):
    def __call__(self, predictions):
        min_endpoint_id = None
        min_value = -1
        for pred in predictions:
            if min_endpoint_id is None or pred["credit"] < min_value:
                min_value = pred["credit"]
                min_endpoint_id = pred["endpoint_id"]
        return min_endpoint_id
    
class MinEnergy(Strategy):
    def __call__(self, predictions):
        min_endpoint_id = None
        min_value = -1
        for pred in predictions:
            if min_endpoint_id is None or pred["energy"] < min_value:
                min_value = pred["energy"]
                min_endpoint_id = pred["endpoint_id"]
        return min_endpoint_id

class MinRuntime(Strategy):
    def __call__(self, predictions):
        min_endpoint_id = None
        min_value = -1
        for pred in predictions:
            if min_endpoint_id is None or pred["runtime"] < min_value:
                min_value = pred["runtime"]
                min_endpoint_id = pred["endpoint_id"]
        return min_endpoint_id

class EndpointStrategy(Strategy):
    def __init__(self, endpoint_id):
        self.endpoint_id = endpoint_id

    def __call__(self, predictions):
        return self.endpoint_id

strategies = {
    "MinCredit": MinCredit,
    "MinEnergy": MinEnergy,
    "MinRuntime": MinRuntime
}
    
def run_tasks(user, 
              tasks,
              strategy,
              result_log_file,
              submit_interval=10,
              polling_interval=2,
              batch_size = 16,
              slots = 32):

    client = GreenAccessClient(user)
    remaining_tasks = len(tasks)
    tasks = reversed(tasks)
    pending_tasks = set()
    start_time = time.time()
    finished_tasks = 0
    submitted_tasks = 0
    task_dict = defaultdict(int)
    allocation_remaining = True

    allocation = client.get_allocation()
    allocation["user"] = user
    allocation["time"] = time.time() - start_time
    allocation["submitted_tasks"] = submitted_tasks
    allocation["finished_tasks"] = finished_tasks
    allocation["strategy"] = type(strategy).__name__
    with open(result_log_file, "a") as fp:
        fp.write(f"{json.dumps(allocation)}\n")

    for _ in range(min(slots, remaining_tasks)):
        (func, args, kwargs) = next(tasks)
        func_id = client.register_function(func, func.__name__)
        predictions = client.predict_func(func)
        endpoint = strategy(predictions)
        #print(predictions)
        try:
            task_id = client.execute_function(endpoint, func_id, *args, **kwargs)
        except:
            allocation_remaining = False
            break

        submitted_tasks += 1
        pending_tasks.add(task_id)
        task_dict[endpoint] += 1
        remaining_tasks -= 1
    
    slots = 0
    last_submit_time = time.time()
    while remaining_tasks > 0 and allocation_remaining:
        time.sleep(polling_interval)
        if time.time() - last_submit_time > submit_interval and slots > min(batch_size, remaining_tasks):
            for _ in range(min(batch_size, remaining_tasks)):
                (func, args, kwargs) = next(tasks)
                func_id = client.register_function(func, func.__name__)
                predictions = client.predict_func(func)
                endpoint = strategy(predictions)
                try:
                    task_id = client.execute_function(endpoint, func_id, *args, **kwargs)
                except Exception as e:
                    allocation_remaining = False
                submitted_tasks += 1
                pending_tasks.add(task_id)
                task_dict[endpoint] += 1
                remaining_tasks -= 1

            slots -= batch_size
            last_submit_time = time.time()

        if len(pending_tasks) > 0:
            task_results = client.get_batch_results(pending_tasks)
            for task_id, result in task_results.items():
                if "result" in result:
                    pending_tasks.remove(task_id)
                    slots += 1
                    finished_tasks += 1
        
        allocation = client.get_allocation()
        allocation["user"] = user
        allocation["time"] = time.time() - start_time
        allocation["submitted_tasks"] = submitted_tasks
        allocation["finished_tasks"] = finished_tasks
        allocation["strategy"] = type(strategy).__name__
        allocation.update(task_dict)
        with open(result_log_file, "a") as fp:
            fp.write(f"{json.dumps(allocation)}\n")

    allocation = client.get_allocation()
    allocation["user"] = user
    allocation["time"] = time.time() - start_time
    allocation["submitted_tasks"] = submitted_tasks
    allocation["finished_tasks"] = finished_tasks
    allocation["strategy"] = type(strategy).__name__
    allocation.update(task_dict)
    with open(result_log_file, "a") as fp:
        fp.write(f"{json.dumps(allocation)}\n")

    while len(pending_tasks) > 0:
        task_results = client.get_batch_results(pending_tasks)
        for task_id, result in task_results.items():
            if "result" in result:
                pending_tasks.remove(task_id)
                slots += 1
                finished_tasks += 1

        time.sleep(polling_interval)
        allocation = client.get_allocation()
        allocation["user"] = user
        allocation["time"] = time.time() - start_time
        allocation["submitted_tasks"] = submitted_tasks
        allocation["finished_tasks"] = finished_tasks
        allocation["strategy"] = type(strategy).__name__
        allocation.update(task_dict)
        with open(result_log_file, "a") as fp:
            fp.write(f"{json.dumps(allocation)}\n")
    
    end_time = time.time()
    # Wait till allocation is deducted. Hopefully this is long enough
    while allocation["jobs_running"] > 0 and time.time() - end_time < 120:
        time.sleep(polling_interval)
        allocation = client.get_allocation()
        allocation["user"] = user
        allocation["time"] = time.time() - start_time
        allocation["submitted_tasks"] = submitted_tasks
        allocation["finished_tasks"] = finished_tasks
        allocation["strategy"] = type(strategy).__name__
        allocation.update(task_dict)
        with open(result_log_file, "a") as fp:
            fp.write(f"{json.dumps(allocation)}\n")
