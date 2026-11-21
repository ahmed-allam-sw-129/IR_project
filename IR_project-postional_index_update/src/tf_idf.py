"""Term Analysis with Spark: TF, DF, IDF, TF-IDF, vector lengths, normalized TF-IDF.

Everything starts from the Positional Index:  term -> {doc_id: [positions]}

Formulas (same as the project PDF):
  raw TF      = number of positions of the term in the document
  weighted TF = 1 + log10(TF)  if TF > 0, else 0
  DF          = number of documents in the term's postings
  IDF         = log10(N / DF)
  TF-IDF      = weighted TF x IDF
  doc length  = sqrt(sum of squared TF-IDF values in that document)
  normalized  = TF-IDF / doc length
"""
import math

from src.spark_loader import get_spark


def compute_term_analysis(index, doc_ids):
    """index   = INDEX from build_positional_index()
    doc_ids = ["d1", "d2", ..., "d10"] (this is the column order)

    Returns a dict with:
      "doc_ids" : the column names
      "rows"    : one dict per term with df, idf, raw, weighted, tfidf, norm
                  (raw / weighted / tfidf / norm are lists, one value per document)
      "lengths" : one dict per document with its vector length
    """
    sc = get_spark().sparkContext
    n_docs = len(doc_ids)

    # Step 1: one record per term -> (term, {doc_id: [positions]})
    terms_rdd = sc.parallelize(list(index.items()))

    # Step 2: raw TF, weighted TF, DF, IDF and TF-IDF for every term
    def make_row(item):
        term, postings = item
        raw = [len(postings.get(d, [])) for d in doc_ids]       # raw TF
        weighted = [1 + math.log10(tf) if tf > 0 else 0.0 for tf in raw]
        df = len(postings)                                       # DF
        idf = math.log10(n_docs / df)                            # IDF
        tfidf = [w * idf for w in weighted]                      # TF-IDF
        return {"term": term, "df": df, "idf": idf,
                "raw": raw, "weighted": weighted, "tfidf": tfidf}

    rows_rdd = terms_rdd.map(make_row).cache()

    # Step 3: document vector length = sqrt(sum of squared TF-IDF) per document
    sums = (rows_rdd
            .flatMap(lambda r: [(i, w * w) for i, w in enumerate(r["tfidf"])])
            .reduceByKey(lambda a, b: a + b)
            .collect())
    lengths = [0.0] * n_docs
    for i, total in sums:
        lengths[i] = math.sqrt(total)

    # Step 4: normalized TF-IDF = TF-IDF / document length
    def add_norm(r):
        r["norm"] = [w / lengths[i] if lengths[i] > 0 else 0.0
                     for i, w in enumerate(r["tfidf"])]
        return r

    rows = rows_rdd.map(add_norm).collect()
    rows.sort(key=lambda r: r["term"])  # terms A-Z

    return {
        "doc_ids": doc_ids,
        "rows": rows,
        "lengths": [{"doc": d, "length": lengths[i]} for i, d in enumerate(doc_ids)],
    }
