import json
import os
import threading
import uuid

import click

from .functions import all_functions, balanced_functions, mainify
from .user import strategies, EndpointStrategy, run_tasks
from green_access import GreenAccessClient

def setup(endpoints, users, init_credits):
    admin_client = GreenAccessClient("admin")
    for user_id in range(users):
        admin_client.register_user(f"user_{user_id}", init_credits)
        # Override creits if user has already been created
        admin_client.set_allocation(f"user_{user_id}", init_credits)

    for endpoint in endpoints:
        admin_client.register_endpoint(**endpoint)

def run_one(ntasks, users, strategy, n_endpoints, functions=balanced_functions, parent_dir=None):
    tasks = []
    for i in range(ntasks):
        func = functions[i % len(functions)]
        func = mainify(func) # Dill serialization hack
        tasks.append((func, (), {}))
    
    run_dir = f"users-{users}-strategy-{strategy}-endpoints-{n_endpoints}"
    if parent_dir:
        run_dir = os.path.join(parent_dir, run_dir)
    else:
        parent_dir = f"run-{str(uuid.uuid4())}"
        os.mkdir(parent_dir)
        run_dir = os.path.join(parent_dir, run_dir)
    os.mkdir(run_dir)

    user_threads = []
    for i in range(users):
        if strategy == "rr":
            strategy_class = list(strategies.values())[i % len(strategies)]
        elif strategy in strategies:
            strategy_class = strategies[strategy]
        else:
            strategy_class = lambda : EndpointStrategy(strategy)

        user_thread = threading.Thread(target=run_tasks,
                                       args=(
                                        f"user_{i}",
                                        tasks.copy(),
                                        strategy_class(),
                                        os.path.join(run_dir, f"user_{i}_trace.jsonl")
                                       ))
        user_threads.append(user_thread)
    
    for t in user_threads:
        t.start()

    for t in user_threads:
        t.join()

    print("Finished!")

@click.group()
def cli():
    pass

@cli.command()
@click.option(
    "--config",
    required=True,
    type=click.Path(readable=True),
    help="Location of endpoint config file.",
)
@click.option(
    "--init-credits", "-i",
    type=int,
    default=1.0,
    help="Initial credits to use"
)
@click.option(
    "--ntasks", "-n",
    type=int,
    default=1,
    help="Number of repitions to include in bag of tasks",
)
@click.option(
    "--users", "-u",
    type=int,
    default=1,
    help="Number of users to run",
)
@click.option(
    "--strategy", "-s",
    type=str,
    default="rr",
    help="Strategy to use",
)
def basic(config, init_credits, ntasks, users, strategy):
    with open(config) as fp:
        endpoints = json.load(fp)

    setup(endpoints, users, init_credits)
    run_one(ntasks, users, strategy, len(endpoints))

@cli.command()
def predict():
    client = GreenAccessClient("admin")
    print("Function\t\t\t\tRuntime\tEnergy\tCredits")
    for func in all_functions:
        func_id = client.register_function(func, func.__name__)
        predictions = client.predict_func(func)
        print(f"{func.__name__}")
        for pred in predictions:
            print(f"\t{pred['endpoint_name']}\t{pred['runtime']}\t{pred['energy']}\t{pred['credit']}")

@cli.command()
@click.option(
    "--config",
    required=True,
    type=click.Path(readable=True),
    help="Location of endpoint config file.",
)
@click.option(
    "--ntasks", "-n",
    type=int,
    default=1,
    help="Number of repitions to include in bag of tasks",
)
def profile(config, ntasks):
    with open(config) as fp:
        endpoints = json.load(fp)

    # Setup user and all endpoints
    setup(endpoints, 1, 50)

    # Run each function ntask number of times on each endpoint
    for endpoint in endpoints: 
        strategy = endpoint["endpoint_id"]
        for function in all_functions:
            run_one(ntasks, 1, strategy, len(endpoints), functions=[function,])

if __name__ == "__main__":
    cli()
