# Linear Algebra Applications
def matrix_multiplication():
    import numpy as np
    from numpy.linalg import cholesky

    a = np.random.rand(4096, 4096)
    b = np.random.rand(4096, 4096)
    c = a @ b 

    return

def cholesy_decomposition():
    import numpy as np
    from numpy.linalg import cholesky

    a = np.random.rand(4096, 4096)
    a = a.T @ a # Make hermatian
    L = cholesky(a)

    return

# Graph Applications
def graph_bfs():
    import igraph

    size = 500000
    graph = igraph.Graph.Barabasi(size, 10)
    result = graph.bfs(0)

    return result

def graph_mst():
    import igraph

    size = 500000
    graph = igraph.Graph.Barabasi(size, 10)
    result = graph.spanning_tree(None, False)

    return result[0]


def graph_pagerank():
    import igraph

    size = 500000
    graph = igraph.Graph.Barabasi(size, 10)
    result = graph.pagerank()

    return result[0]

# Scientific Applications
def molecular_dynamics_ie(smiles = ["CCC", "C#CC12OC3CC1C2O3", "CC1=NC(O)=NC=C1F"]):
    from chemfunctions import compute_vertical

    results = []
    for mol in smiles:
        results.append(compute_vertical(mol))
    
    return results

def dna_visualization(url="https://raw.githubusercontent.com/spcl/serverless-benchmarks-data/6a17a460f289e166abb47ea6298fb939e80e8beb/500.scientific/504.dna-visualisation/bacillus_subtilis.fasta"):
    from squiggle import transform
    import urllib.request
    import tempfile
    import os

    (_, file_name) = tempfile.mkstemp()
    urllib.request.urlretrieve(url, file_name)
    data = open(file_name, "r").read()
    result = transform(data)

    (_, output_path) = tempfile.mkstemp()
    with open(output_path, "w") as fp:
        json.dump(result, fp)

    os.remove(file_name)
    os.remove(output_path)

    return output_path

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
    
all_functions = [matrix_multiplication, 
             cholesy_decomposition,
             graph_bfs,
             graph_mst,
             graph_pagerank,
             molecular_dynamics_ie,
             dna_visualization
            ]

balanced_functions = [
    matrix_multiplication,
    cholesy_decomposition,
    graph_pagerank,
    molecular_dynamics_ie
]