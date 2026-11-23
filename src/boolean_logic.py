"""Boolean logic on phrase-match sets (Boolean Logic page).

A = documents containing phrase A, B = documents containing phrase B.
  AND      -> A ∩ B   (documents in both sets)
  AND NOT  -> A − B   (documents in A but not in B)
"""
from src.spark_loader import get_spark, natural_key
from src.search import phrase_search


def run_boolean(index, documents, phrase_a, phrase_b, op):
    docs_a = [m["doc"] for m in phrase_search(index, phrase_a)]
    docs_b = [m["doc"] for m in phrase_search(index, phrase_b)]

    # Spark does the set operations
    sc = get_spark().sparkContext
    rdd_a = sc.parallelize(docs_a)
    rdd_b = sc.parallelize(docs_b)
    both = rdd_a.intersection(rdd_b).collect()   # A ∩ B
    only_a = rdd_a.subtract(rdd_b).collect()     # A − B
    only_b = rdd_b.subtract(rdd_a).collect()     # B − A

    both = sorted(both, key=natural_key)
    only_a = sorted(only_a, key=natural_key)
    only_b = sorted(only_b, key=natural_key)
    result = both if op == "AND" else only_a

    texts = {d["id"]: d["text"] for d in documents}
    return {
        "a": phrase_a, "b": phrase_b, "op": op,
        "docs_a": sorted(docs_a, key=natural_key),
        "docs_b": sorted(docs_b, key=natural_key),
        "only_a": only_a, "only_b": only_b, "both": both,
        "result": result,
        "matching": [{"rank": i, "doc": d, "text": texts.get(d, "")}
                     for i, d in enumerate(result, start=1)],
    }