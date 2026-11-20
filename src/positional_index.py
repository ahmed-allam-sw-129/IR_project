"""Build the Positional Index with Spark.

Result shape:  term -> document -> [1-based positions]
Example:       "care" -> {"d1": [2], "d5": [2, 8], "d6": [5], "d9": [6]}
"""
from src.spark_loader import get_spark


def build_positional_index(documents):
    """documents = the list from load_dataset()["documents"].
    Each item has an "id" (like "d1") and "tokens" (like [(1, "patient"), (2, "care")]).
    """
    sc = get_spark().sparkContext

    # (doc_id, [(position, term), ...])
    docs_rdd = sc.parallelize([(d["id"], d["tokens"]) for d in documents])

    # Step 1: one record per token ->  ((term, doc_id), position)
    pairs = docs_rdd.flatMap(
        lambda d: [((term, d[0]), pos) for pos, term in d[1]]
    )

    # Step 2: collect all positions of a term inside one document
    #         ((term, doc_id), [positions])
    per_doc = pairs.groupByKey().mapValues(lambda p: sorted(p))

    # Step 3: collect all documents of a term
    #         (term, [(doc_id, [positions]), ...])
    per_term = per_doc.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey()

    # Bring the result back to Python and sort it (terms A-Z, docs d1, d2, ...)
    index = {}
    for term, postings in per_term.collect():
        postings = sorted(postings, key=lambda p: int(p[0][1:]))
        index[term] = {doc_id: positions for doc_id, positions in postings}

    return dict(sorted(index.items()))


def index_for_page(index):
    """Turn the index into a simple list the HTML table can loop over."""
    rows = []
    for term, postings in index.items():
        rows.append({
            "term": term,
            "df": len(postings),  # DF = number of documents that contain the term
            "postings": [{"doc": d, "positions": p} for d, p in postings.items()],
        })
    return rows
