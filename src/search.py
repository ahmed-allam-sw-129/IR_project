"""Phrase search + query vector + ranking (Search & Ranking page).

Everything starts from the Positional Index (INDEX) and the Term Analysis (TERMS).
Formulas (same as the project PDF):
  query weighted TF = 1 + log10(TF in the query)
  query TF-IDF      = weighted TF x collection IDF
  query length      = sqrt(sum of squared query TF-IDF)
  normalized query  = query TF-IDF / query length
  similarity        = dot product of normalized query and normalized document vector
"""
import math

from src.spark_loader import get_spark, natural_key


def phrase_search(index, phrase):
    """Exact, contiguous phrase matching using the Positional Index.

    Returns a list (sorted d1, d2, ...) like:
      [{"doc": "d5", "starts": [1, 7], "ranges": ["1-2", "7-8"]}, ...]
    """
    words = phrase.lower().split()
    if not words or any(w not in index for w in words):
        return []  # empty phrase, or a word that is not in the collection

    # Candidate documents = documents that contain ALL the words
    candidates = set(index[words[0]])
    for w in words[1:]:
        candidates = candidates & set(index[w])

    # (doc_id, [positions of word 1, positions of word 2, ...])
    data = [(doc, [index[w][doc] for w in words]) for doc in sorted(candidates, key=natural_key)]

    # Spark checks every candidate document.
    # A start position p is a match if word i is at position p + i for every i.
    sc = get_spark().sparkContext
    found = (sc.parallelize(data)
             .map(lambda d: (d[0], [p for p in d[1][0]
                                    if all((p + i) in d[1][i] for i in range(len(d[1])))]))
             .filter(lambda d: len(d[1]) > 0)
             .collect())
    found.sort(key=lambda d: natural_key(d[0]))

    n = len(words)
    return [{"doc": doc,
             "starts": starts,
             "ranges": [str(s) + "-" + str(s + n - 1) for s in starts]}
            for doc, starts in found]


def build_query_vector(phrase, terms):
    """Weight the query terms with the collection IDF.

    Returns {"rows": [{"term", "tf", "weighted", "tfidf", "norm"}], "length": ...}
    Terms that are not in the collection are ignored.
    """
    idf = {r["term"]: r["idf"] for r in terms["rows"]}

    # count each term inside the query (keep the order of first appearance)
    counts = {}
    for w in phrase.lower().split():
        if w in idf:
            counts[w] = counts.get(w, 0) + 1

    rows = []
    for term, tf in counts.items():
        weighted = 1 + math.log10(tf)
        rows.append({"term": term, "tf": tf, "weighted": weighted,
                     "tfidf": weighted * idf[term]})

    length = math.sqrt(sum(r["tfidf"] ** 2 for r in rows))
    for r in rows:
        r["norm"] = r["tfidf"] / length if length > 0 else 0.0

    return {"rows": rows, "length": length}


def rank_documents(query, terms, doc_ids):
    """Similarity of the query with the given (matching) documents, highest first.

    query   = result of build_query_vector()
    doc_ids = the matching documents, like ["d1", "d5", "d6"]
    Returns [{"rank": 1, "doc": "d1", "score": 1.0}, ...]
    """
    by_term = {r["term"]: r for r in terms["rows"]}
    scores = []
    for doc in doc_ids:
        i = terms["doc_ids"].index(doc)  # column of this document
        score = sum(q["norm"] * by_term[q["term"]]["norm"][i] for q in query["rows"])
        scores.append({"doc": doc, "score": score})

    # highest score first (if equal, natural document order)
    scores.sort(key=lambda s: (-s["score"], natural_key(s["doc"])))
    for n, s in enumerate(scores, start=1):
        s["rank"] = n
    return scores


def run_search(index, terms, phrase):
    """All steps for one phrase: matches, query vector and ranking."""
    matches = phrase_search(index, phrase)
    query = build_query_vector(phrase, terms)
    ranking = rank_documents(query, terms, [m["doc"] for m in matches])
    return {"phrase": phrase, "matches": matches, "query": query, "ranking": ranking}