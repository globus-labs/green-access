import requests
import time

from green_access import GreenAccessClient
base_url = "http://127.0.0.1:8000"

def test_liveness():
    client = GreenAccessClient("admin", url=base_url)
    assert client.liveness()

def test_admin():
    client = GreenAccessClient("admin", url=base_url)
    assert client.register_user("vhayot", 600)
    assert client.register_user("maxime_gonthier", 200)
    assert client.set_allocation("vhayot", 200)

    endpoint = {
        "endpoint_id": "6754af96-7afa-4c81-b7ef-cf54587f02fa",
        "display_name": "Desktop",
        "nodes": 1,
        "cores_per_node": 16,
        "per_node_tdp": 65,
        "embodied_carbon_rate": 376,
        "carbon_intensity": 502,
        "idle_power": 5.0,
        "lat": 41.78958,
        "long": -87.60093
    }
    assert client.register_endpoint(**endpoint)

    assert client.register_user("admin", 600)
    allocation = client.get_allocation() # Test that register worked

def test_allocations():
    client = GreenAccessClient("admin", url=base_url)
    assert client.register_user("admin", 600)
    allocation = client.get_allocation()
    
    # These are internal methods, so they are not supported
    # by the client

    # Try allocation that should fail
    params = {"username": "admin"}
    execute_req = {
        "endpoint_id": "6754af96-7afa-4c81-b7ef-cf54587f02fa",
        "runtime": 60,
        "energy": 60 * 30,
        "credit": allocation["credits_remaining"] + 10
    }
    resp = requests.post(f"{base_url}/allocation/execute", params=params, json=execute_req)
    assert resp.status_code == 405 # Insufficient credits

    # Try allocation that should succeed
    params = {"username": "admin"}
    execute_req = {
        "endpoint_id": "6754af96-7afa-4c81-b7ef-cf54587f02fa",
        "runtime": 60,
        "energy": 60 * 30,
        "credit": 1
    }
    resp = requests.post(f"{base_url}/allocation/execute", params=params, json=execute_req)
    assert resp.ok

    # Check allocation
    updated_allocation = client.get_allocation()
    assert updated_allocation["credits_remaining"] == allocation["credits_remaining"] - 1
    assert updated_allocation["credits_pending"] == allocation["credits_pending"] + 1
    assert updated_allocation["jobs_running"] == allocation["jobs_running"] + 1

    # Mark job as finished
    params = {"username": "admin"}
    deduct_req = {
        "credits_predicted": 1,
        "credits_consumed": 0.5,
        "energy_consumed": 0.5,
        "core_hours_consumed": 1
    }
    resp = requests.post(f"{base_url}/allocation/deduct", params=params, json=deduct_req)
    assert resp.ok

    # Check allocation
    updated_allocation_2 = client.get_allocation()
    assert updated_allocation_2["credits_remaining"] == updated_allocation["credits_remaining"] + 0.5
    assert updated_allocation_2["credits_pending"] == updated_allocation["credits_pending"] - 1
    assert updated_allocation_2["credits_consumed"] == updated_allocation["credits_consumed"] + 0.5
    assert updated_allocation_2["jobs_running"] == updated_allocation["jobs_running"] - 1
    assert updated_allocation_2["jobs_completed"] == updated_allocation["jobs_completed"] + 1

def add(a, b):
    import time
    time.sleep(10)
    return a + b

def mainify(obj):
    """If obj is not defined in __main__ then redefine it in 
    main so that dill will serialize the definition along with the object"""
    if obj.__module__ != "__main__":
        import __main__
        import inspect
        s = inspect.getsource(obj)
        co = compile(s, '<string>', 'exec')
        exec(co, __main__.__dict__)
        return __main__.__dict__.get(obj.__name__)
    return obj

add = mainify(add) # To work with pytest

def test_register():
    client = GreenAccessClient("admin", url=base_url)
    func_id = client.register_function(add, "add")
    assert func_id is not None

def test_predict():
    client = GreenAccessClient("admin", url=base_url)
    func_id = client.register_function(add, "add")

    endpoint = {
        "endpoint_id": "6754af96-7afa-4c81-b7ef-cf54587f02fa",
        "display_name": "Desktop",
        "nodes": 1,
        "cores_per_node": 16,
        "per_node_tdp": 65,
        "embodied_carbon_rate": 376,
        "idle_power": 5.0,
        "lat": 41.78958,
        "long": -87.60093
    }
    assert client.register_endpoint(**endpoint)
    
    predictions = client.predict_func(func_id)
    for prediction in predictions:
        assert prediction["runtime"]
        assert prediction["energy"]
        assert prediction["credit"]

def test_functions():
    client = GreenAccessClient("admin", url=base_url)
    functions = client.get_functions()
    assert len(functions) > 0, f"Received {functions}"

    for function in functions:
        assert "name" in function, f"Invalid function: {function}"
        assert "function_id" in function, f"Invalid function: {function}"

def test_end_to_end():
    client = GreenAccessClient("admin", url=base_url)
    assert client.register_user("admin", 600)

    endpoint_id = "6754af96-7afa-4c81-b7ef-cf54587f02fa"
    endpoint = {
        "endpoint_id": "6754af96-7afa-4c81-b7ef-cf54587f02fa",
        "display_name": "Desktop",
        "nodes": 1,
        "cores_per_node": 16,
        "per_node_tdp": 65,
        "embodied_carbon_rate": 376,
        "idle_power": 5.0,
        "lat": 41.78958,
        "long": -87.60093
    }
    assert client.register_endpoint(**endpoint)

    func_id = client.register_function(add, "add")
    task_id = client.execute_function(endpoint_id, add, 1, 2)
    task_id_2 = client.execute_function(endpoint_id, add, 1, 2)
    pending = client.get_status(task_id)["pending"]
    while pending:
        time.sleep(1)
        pending = client.get_status(task_id)["pending"]

    result = client.get_result(task_id)
    assert result == 3

    results = client.get_batch_results([task_id, task_id_2])
    assert results[task_id]["result"] == 3
