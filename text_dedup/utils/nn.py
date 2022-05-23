#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date         : 2021-06-05 10:53:26
# @Author       : Chenghao Mou (mouchenghao@gmail.com)


import os
from typing import List, Optional

import numpy as np
from annoy import AnnoyIndex
from datasketch import MinHash, MinHashLSH
from simhash import Simhash, SimhashIndex
from mpire import WorkerPool

def annoy_clustering(
    embeddings: List[np.ndarray],
    f: int = 128,
    metric: str = "angular",
    num_trees: int = 64,
    top_k: int = 100,
    distance_threshold: float = 0.5,
    query_embeddings: Optional[List[np.ndarray]] = None,
) -> List[List[int]]:
    """Cluster embeddings with annoy.

    Parameters
    ----------
    embeddings : List[np.ndarray]
        List of embeddings
    f : int, optional
        Number of the embedding features, by default 128
    metric : str, optional
        Metric for distance measurement, by default "angular"
    num_trees : int, optional
        Number of the trees for annoy to build, by default 64
    top_k : int, optional
        Top k values to be returned by annoy, by default 100
    distance_threshold : float, optional
        Distance threshold, by default 0.5

    Returns
    -------
    List[int]
        List of neighbors
    """
    t = AnnoyIndex(f, metric)
    for i, v in enumerate(embeddings):
        t.add_item(i, v)
    t.build(num_trees)

    neighbors: List[List[int]] = []

    if query_embeddings is None:
        query_embeddings = embeddings

    for i, v in enumerate(embeddings):
        current: List[int] = []
        for j, dist in zip(
            *t.get_nns_by_vector(v, top_k, search_k=-1, include_distances=True)
        ):
            if dist < distance_threshold:
                current.append(j)
        neighbors.append(current[:])

    return neighbors


def lsh_clustering(
    signatures: List[np.ndarray],
    threshold: float = 0.5,
    num_perm: int = 128,
    query_signatures: Optional[List[np.ndarray]] = None,
):
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    with lsh.insertion_session() as session:
        for key, minhash in enumerate(signatures):
            session.insert(f"id-{key}", MinHash(num_perm=num_perm, hashvalues=minhash))

    neighbors: List[List[int]] = []

    if query_signatures is None:
        query_signatures = signatures
    neighbors: List[List[int]] = []
    with WorkerPool(n_jobs=os.cpu_count()) as pool:
        neighbors = pool.map(lambda *signature: [int(x.split("-")[1]) for x in lsh.query(MinHash(num_perm=num_perm, hashvalues=signature))], query_signatures)

    return neighbors


def simhash_clustering(
    signatures: List[int],
    hamming_distance: int = 3,
    # num_blocks: Optional[int] = 5,
    query_signatures: Optional[List[int]] = None,
) -> List[List[int]]:

    index = SimhashIndex(
        [(i, Simhash(value=signature)) for i, signature in enumerate(signatures)],
        k=hamming_distance,
    )

    if query_signatures is None:
        query_signatures = signatures

    neighbors: List[List[int]] = []
    with WorkerPool(n_jobs=os.cpu_count()) as pool:
        neighbors = pool.map(lambda signature: list(map(int, index.get_near_dups(Simhash(value=signature)))), query_signatures)

    return neighbors


if __name__ == "__main__":

    print(simhash_clustering([1, 1024, 1231241, 1, 1025]))