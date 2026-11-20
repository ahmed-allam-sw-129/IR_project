"""Load the healthcare documents (docs/d1.txt ... d10.txt) with PySpark.

Rules (same as the project PDF):
  - text is lowercased
  - tokenized by whitespace
  - positions are 1-based
"""
import os
import sys
from pyspark.sql import SparkSession

# Tell Spark to use the same Python that runs app.py
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(BASE_DIR, "docs")


def get_spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("HealthcareIR")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_dataset():
    """Read the documents with Spark and return everything the Dataset page needs."""
    spark = get_spark()
    sc = spark.sparkContext

    # Python reads the files (works with any Windows path), Spark processes them.
    # -> [("d1", "patient care"), ("d2", "patient safety ..."), ...]
    files = []
    for name in os.listdir(DOCS_DIR):
        if name.endswith(".txt"):
            with open(os.path.join(DOCS_DIR, name), encoding="utf-8") as f:
                files.append((name[:-4], f.read()))

    # -> (doc_id, [tokens])   e.g. ("d1", ["patient", "care"])
    docs_rdd = sc.parallelize(files).map(
        lambda f: (f[0], f[1].strip().lower().split())
    )

    docs = docs_rdd.collect()
    docs.sort(key=lambda d: int(d[0][1:]))  # d1, d2, ... d10

    all_tokens = docs_rdd.flatMap(lambda d: d[1])
    total_tokens = all_tokens.count()
    unique_terms = all_tokens.distinct().count()

    documents = []
    for doc_id, tokens in docs:
        documents.append({
            "id": doc_id,
            "text": " ".join(tokens),
            "word_count": len(tokens),
            # [(1, "patient"), (2, "care")] -> 1-based positions
            "tokens": list(enumerate(tokens, start=1)),
        })

    return {
        "documents": documents,
        "stats": {
            "documents": len(documents),
            "unique_terms": unique_terms,
            "total_tokens": total_tokens,
        },
    }