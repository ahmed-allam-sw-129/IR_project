"""Load documents from a path with PySpark.

Rules (same as the project PDF):
  - text is lowercased
  - tokenized by whitespace
  - positions are 1-based

Supported files (a single file or a whole folder):
  .txt .md .log          one file = one document
  .pdf .docx             one file = one document (text is extracted)
  .csv .tsv .json .jsonl one row / record = one document
"""
import csv
import json
import os
import re
import sys
from pyspark.sql import SparkSession

# Tell Spark to use the same Python that runs app.py
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DOCS_DIR = os.path.join(BASE_DIR, "docs")

TEXT_EXT = {".txt", ".md", ".log"}
TABLE_EXT = {".csv", ".tsv", ".json", ".jsonl"}
BINARY_EXT = {".pdf", ".docx"}
SUPPORTED = TEXT_EXT | TABLE_EXT | BINARY_EXT

# For csv/json: the column that holds the document text (first match wins).
# If none of these exists, all the values of the row are joined together.
TEXT_COLUMNS = ["text", "content", "body", "description", "abstract",
                "review", "message", "notes", "note"]
ID_COLUMNS = ["doc_id", "id"]


def natural_key(name):
    """d2 comes before d10 (instead of 'd10' < 'd2')."""
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", name)]


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


# ---------- one reader per kind of file: each returns [(doc_id, text), ...] ----------

def _read_text(path, stem):
    with open(path, encoding="utf-8", errors="replace") as f:
        return [(stem, f.read())]


def _read_pdf(path, stem):
    try:
        from pypdf import PdfReader
    except ImportError:
        raise ValueError("PDF support needs:  pip install pypdf")
    reader = PdfReader(path)
    return [(stem, " ".join((p.extract_text() or "") for p in reader.pages))]


def _read_docx(path, stem):
    try:
        from docx import Document
    except ImportError:
        raise ValueError("Word support needs:  pip install python-docx")
    doc = Document(path)
    parts = [p.text for p in doc.paragraphs]
    for table in doc.tables:                       # text inside tables too
        for row in table.rows:
            parts.extend(cell.text for cell in row.cells)
    return [(stem, " ".join(parts))]


def _records_to_docs(stem, records):
    """records = list of dicts (rows of a csv / objects of a json file)."""
    docs = []
    for n, rec in enumerate(records, start=1):
        if isinstance(rec, dict):
            lower = {str(k).lower(): v for k, v in rec.items()}
            text_key = next((c for c in TEXT_COLUMNS if c in lower), None)
            id_key = next((c for c in ID_COLUMNS if c in lower and lower[c] not in (None, "")), None)
            if text_key:
                text = str(lower[text_key] or "")
            else:
                text = " ".join(str(v) for v in lower.values() if v not in (None, ""))
            doc_id = str(lower[id_key]) if id_key else f"{stem}_{n}"
        else:                                       # json list of plain strings
            text, doc_id = str(rec), f"{stem}_{n}"
        docs.append((doc_id, text))
    return docs


def _read_table(path, stem, ext):
    if ext in (".csv", ".tsv"):
        delimiter = "\t" if ext == ".tsv" else ","
        with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
            return _records_to_docs(stem, list(csv.DictReader(f, delimiter=delimiter)))

    if ext == ".jsonl":
        with open(path, encoding="utf-8-sig", errors="replace") as f:
            return _records_to_docs(stem, [json.loads(line) for line in f if line.strip()])

    # .json: a list of records, or an object containing a list, or one record
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        data = json.load(f)
    if isinstance(data, dict):
        lists = [v for v in data.values() if isinstance(v, list)]
        data = lists[0] if lists else [data]
    return _records_to_docs(stem, data)


def _read_file(path):
    stem, ext = os.path.splitext(os.path.basename(path))
    ext = ext.lower()
    if ext in TEXT_EXT:
        return _read_text(path, stem)
    if ext == ".pdf":
        return _read_pdf(path, stem)
    if ext == ".docx":
        return _read_docx(path, stem)
    if ext in TABLE_EXT:
        return _read_table(path, stem, ext)
    raise ValueError(f"Unsupported file type: {ext}")


# ---------- the loader used by load_dataset ----------

def read_documents(path):
    """Return [(doc_id, text), ...] from a single file or a whole folder."""
    if os.path.isfile(path):
        paths = [path]
    elif os.path.isdir(path):
        paths = [os.path.join(path, n) for n in sorted(os.listdir(path), key=natural_key)
                 if os.path.splitext(n)[1].lower() in SUPPORTED]
    else:
        raise FileNotFoundError(f"Path not found: {path}")

    files = []
    for p in paths:
        try:
            files.extend(_read_file(p))
        except ValueError:
            raise
        except Exception as e:                      # corrupt file, bad json, ...
            raise ValueError(f"Could not read {os.path.basename(p)}: {e}")

    # skip empty documents
    files = [(i, t) for i, t in files if t and t.strip()]
    if not files:
        raise ValueError(f"No supported documents found in: {path}")

    # make sure every document id is unique (a.txt + a.pdf, repeated csv ids, ...)
    seen, unique = {}, []
    for doc_id, text in files:
        if doc_id in seen:
            seen[doc_id] += 1
            doc_id = f"{doc_id}_{seen[doc_id]}"
        else:
            seen[doc_id] = 1
        unique.append((doc_id, text))
    return unique


def load_dataset(path=DEFAULT_DOCS_DIR):
    """Read the documents at `path` with Spark and return what the Dataset page needs."""
    spark = get_spark()
    sc = spark.sparkContext

    files = read_documents(path)

    # -> (doc_id, [tokens])   e.g. ("d1", ["patient", "care"])
    docs_rdd = sc.parallelize(files).map(
        lambda f: (f[0], f[1].strip().lower().split())
    )

    docs = docs_rdd.collect()
    docs.sort(key=lambda d: natural_key(d[0]))   # d1, d2, ... d10

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
        "path": path,
        "documents": documents,
        "stats": {
            "documents": len(documents),
            "unique_terms": unique_terms,
            "total_tokens": total_tokens,
        },
    }