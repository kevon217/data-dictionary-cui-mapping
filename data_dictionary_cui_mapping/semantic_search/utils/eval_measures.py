"""

Evaluation measures for umls dense vector embedding query results.

"""

from math import log2
import numpy as np


def cosine_similarity(vecs):
    """Calculate cosine similarity between vectors."""

    cos_sim = np.zeros((vecs.shape[0], vecs.shape[0]))
    for i, vec in enumerate(vecs):
        cos_sim[i, :] = np.dot(vec, vecs.T) / (
            np.linalg.norm(vec) * np.linalg.norm(vecs, axis=1)
        )
    return cos_sim


def recall(actual: list, predicted: list, K: int):
    """Calculate recall@K."""

    act_set = set(actual)
    pred_set = set(predicted[:K])
    result = round(len(act_set & pred_set) / float(len(act_set)), 2)
    return result


def mrr(actual_relevant):
    """Calculate mean reciprocal rank."""

    # number of queries
    Q = len(actual_relevant)

    # calculate the reciprocal of the first actual relevant rank
    reciprocal = 0
    for q in range(Q):
        if len(actual_relevant[q]) == 0:
            reciprocal = reciprocal + 0
        else:
            first_result = actual_relevant[q][0]
            reciprocal = reciprocal + (1 / first_result)
            # print(f"query #{q+1} = 1/{first_result} = {reciprocal}")

    # calculate mrr
    mrr = 1 / Q * reciprocal

    # generate results
    # print("MRR =", round(mrr, 2))
    return mrr


def mapk(actual, predicted, K):
    """Calculate mean average precision."""

    Q = len(actual)
    ap = []

    # loop through and calculate AP for each query q
    for q in range(Q):
        ap_num = 0
        # loop through k values
        for k in range(K):
            # calculate precision@k
            act_set = set(actual[q])
            pred_set = set(predicted[: k + 1])
            precision_at_k = len(act_set & pred_set) / (k + 1)
            # calculate rel_k values
            if predicted[k] in actual[q]:
                rel_k = 1
            else:
                rel_k = 0
            # calculate numerator value for ap
            ap_num += precision_at_k * rel_k
        # now we calculate the AP value as the average of AP
        # numerator values
        if len(actual[q]) == 0:
            ap_q = 0
        else:
            ap_q = ap_num / len(actual[q])
        # print(f"AP@{K}_{q + 1} = {round(ap_q, 2)}")
        ap.append(ap_q)

    # now we take the mean of all ap values to get mAP
    map_at_k = sum(ap) / Q

    # generate results
    return map_at_k
