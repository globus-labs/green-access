import requests
import json

from globus_compute_sdk import Client

class TaskPending(Exception):
    """Task is pending and no result is available yet"""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Task is pending due to {self.reason}"

class GreenAccessClient:
    def __init__(self, username, url="http://127.0.0.1:8000", gc_proxy_client = None):
        self.username = username
        self.url = url
        self.function_cache = {}

        if gc_proxy_client is None:
            self.gc_client = Client()
        else:
            self.gc_client = Client()
    
    def liveness(self):
        params = {"username": self.username}
        resp = requests.get(f"{self.url}/", params=params)
        if resp.status_code != 200:
            raise Exception(f"Register user failed: {resp.reason}")
        return True

    def register_user(self, new_user, init_credits):
        params = {"username": self.username}
        body = {
            "new_user": {"username": str(new_user)}, 
            "init_credits": init_credits
        }
        resp = requests.post(f"{self.url}/admin/register_user", params=params, json=body)
        if not resp.ok:
            raise Exception(f"Register user failed: {resp.reason}")
        return True
    
    def set_allocation(self, user, credit):
        params = {"username": self.username}
        body = {
            "user": {"username": str(user)}, 
            "credit": credit
        }
        resp = requests.post(f"{self.url}/admin/set_allocation", params=params, json=body)
        if not resp.ok:
            raise Exception(f"Register user failed: {resp.reason}")
        return True

    def register_endpoint(self, 
                          endpoint_id,
                          display_name,
                          nodes,
                          cores_per_node,
                          per_node_tdp,
                          embodied_carbon_rate,
                          carbon_intensity,
                          idle_power,
                          lat,
                          long):
        params = {"username": self.username}
        body = {
            "endpoint_id": endpoint_id,
            "display_name": display_name,
            "nodes": nodes,
            "cores_per_node": cores_per_node,
            "per_node_tdp": per_node_tdp,
            "embodied_carbon_rate": embodied_carbon_rate,
            "carbon_intensity": carbon_intensity,
            "idle_power": idle_power,
            "lat": lat,
            "long": long
        }
        resp = requests.post(f"{self.url}/admin/register_endpoint", params=params, json=body)
        if not resp.ok:
            raise Exception(f"Register endpoint failed: {resp.reason}")
        
        return True
            
    def get_allocation(self):
        params = {"username": self.username}
        resp = requests.get(f"{self.url}/allocation/", params=params)
        if resp.status_code != 200:
            raise Exception(f"Retrieving allocation failed: {resp.reason}")
        return resp.json()

    def register_function(self, fn, fn_name):
        if fn in self.function_cache:
            return self.function_cache[fn]

        params = {"username": self.username}
        payload = self.gc_client.fx_serializer.serialize(fn)
        register_body = {
            "function_name": fn_name,
            "payload": payload
        }
        resp = requests.post(f"{self.url}/register_function", params=params, json=register_body)
        if resp.status_code != 200:
            raise Exception(f"Register function failed: {resp.reason}, {resp.json()}")
        
        func_id = resp.json()["function_id"]
        self.function_cache[fn] = func_id
        return func_id
        
    def execute_function(self, endpoint_id, fn, *args, **kwargs):        
        if callable(fn):
            if fn in self.function_cache:
                fn_id = self.function_cache[fn]
            else:
                fn_id = self.register_function(fn, fn.__name__)
        else:
            assert isinstance(fn, str)
            fn_id = fn
        
        params = {"username": self.username}
        payload = self.gc_client.fx_serializer.serialize((args, kwargs))
        exec_body = {
            "function_id": fn_id,
            "endpoint_id": endpoint_id,
            "payload": payload
        }
        resp = requests.post(f"{self.url}/execute_function", params=params, json=exec_body)
        if resp.status_code != 200:
            raise Exception(f"Register function failed: {resp.reason}")
        
        return resp.json()['task_id']
    
    def predict_func(self, fn):
        if callable(fn):
            if fn not in self.function_cache:
                raise Exception("ArgumentError: cannot predict unseen function")
            fn_id = self.function_cache[fn]
        else:
            fn_id = fn
        
        params = {"function_id": fn_id}
        resp = requests.get(f"{self.url}/predict/", params=params)
        if resp.status_code != 200:
            raise Exception(f"Retrieving allocation failed: {resp.reason}")
        return resp.json()["predictions"]

    def get_status(self, task_id):
        params = {"username": self.username, "task_id": task_id}
        resp = requests.get(f"{self.url}/status", params=params)
        if resp.status_code != 200:
            raise Exception(f"Retrieving allocation failed: {resp.reason}")
        
        r_dict = resp.json()
        pending = r_dict["status"] not in ("success", "failed")
        r_dict["pending"] = pending
        return r_dict

    def _get_result_helper(self, r_dict):
        pending = r_dict["status"] not in ("success", "failed")
        r_dict["pending"] = pending
        if not pending:
            if "result" in r_dict:
                try:
                    r_obj = self.gc_client.fx_serializer.deserialize(r_dict["result"])
                except Exception:
                    raise Exception("Result Object Deserialization")
                else:
                    r_dict["result"] = r_obj

            elif "exception" in r_dict:
                raise Exception(r_dict["exception"])
            else:
                raise NotImplementedError("unreachable")
        
        return r_dict
        
        
    def get_result(self, task_id):
        params = {"username": self.username, "task_id": task_id}
        resp = requests.get(f"{self.url}/status", params=params)
        if resp.status_code != 200:
            raise Exception(f"Retrieving allocation failed: {resp.reason}")
        result = resp.json()
        response = self._get_result_helper(result)     

        if response["pending"]:
            raise TaskPending(response.get("reason", "unknown")) 
        return response["result"]

    def get_batch_results(self, task_id_list):
        params = {"username": self.username, "task_ids": task_id_list}
        resp = requests.get(f"{self.url}/batch/status", params=params)
        if resp.status_code != 200:
            raise Exception(f"Retrieving allocation failed: {resp.reason}")
        r = resp.json()

        results = {}
        for task_id in task_id_list:
            try:
                data = r["results"][task_id]
                rets = self._get_result_helper(data)
                results[task_id] = rets
            except KeyError:
                raise KeyError(f"Task {task_id} info was not available in the batch status")

        return results

    def get_functions(self):
        params = {"username": self.username}
        resp = requests.get(f"{self.url}/functions", params=params)
        if resp.status_code != 200:
            raise Exception(f"Retrieving allocation failed: {resp.reason}")
        return resp.json()
