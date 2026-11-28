import os

from flask import Flask, render_template, request, redirect, url_for

from src.spark_loader import load_dataset
from src.positional_index import build_positional_index, index_for_page
from src.tf_idf import compute_term_analysis
from src.search import run_search
from src.boolean_logic import run_boolean

app = Flask(
    __name__,
    template_folder="web_ui/templates",
    static_folder="web_ui/static",
)

# The current dataset. These three are replaced whenever a new path is loaded.
DATA = INDEX = TERMS = None


def build_corpus(path=None):
    """Load the documents at `path` and rebuild the index and term analysis."""
    global DATA, INDEX, TERMS
    # Build everything into local variables first. If loading fails,
    # the current dataset stays untouched.
    data = load_dataset(path) if path else load_dataset()
    index = build_positional_index(data["documents"])  # term -> {doc: [positions]}
    # TF, DF, IDF, TF-IDF, vector lengths, normalized TF-IDF (all start from index)
    terms = compute_term_analysis(index, [d["id"] for d in data["documents"]])
    DATA, INDEX, TERMS = data, index, terms


build_corpus()  # default docs/ folder at startup

# The two phrases assigned in the project
ASSIGNED = ["patient care", "medical research"]


@app.context_processor
def inject_dataset_name():
    """Makes `dataset_name` available in every template (base.html included)."""
    path = DATA["path"] if DATA else ""
    name = os.path.basename(path.replace("\\", "/").rstrip("/"))
    return {"dataset_name": name or "dataset"}


@app.route("/")
def overview():
    # Preview card: run one of the assigned phrases on the loaded dataset.
    # Use the first phrase that has matches, otherwise the first phrase.
    result = None
    for phrase in ASSIGNED:
        r = run_search(INDEX, TERMS, phrase)
        if result is None:
            result = r
        if r["matches"]:
            result = r
            break

    return render_template(
        "overview.html",
        page="overview",
        stats=DATA["stats"],
        preview={
            "phrase": result["phrase"],
            "rows": result["ranking"][:3],        # top 3 documents
            "total": len(result["matches"]),
            "terms": len(result["query"]["rows"]),
        },
    )


@app.route("/dataset")
def dataset():
    return render_template(
        "dataset.html",
        page="dataset",
        stats=DATA["stats"],
        documents=DATA["documents"],
        path=DATA["path"],
        error=None,
    )


@app.route("/load", methods=["POST"])
def load_path():
    # strip quotes: Windows "Copy as path" adds them
    path = request.form.get("path", "").strip().strip('"')
    error = None
    if not path:
        error = "Please enter a path."
    else:
        try:
            build_corpus(path)
            return redirect(url_for("dataset"))
        except (FileNotFoundError, ValueError) as e:
            error = str(e)

    # Loading failed: show the page again with the old dataset and the error
    return render_template(
        "dataset.html",
        page="dataset",
        stats=DATA["stats"],
        documents=DATA["documents"],
        path=DATA["path"],
        error=error,
    )


@app.route("/positional-index")
def positional_index():
    return render_template(
        "positional_index.html",
        page="positional_index",
        stats=DATA["stats"],
        rows=index_for_page(INDEX),
    )


@app.route("/term-analysis")
def term_analysis():
    return render_template(
        "term_analysis.html",
        page="term_analysis",
        stats=DATA["stats"],
        terms=TERMS,
    )


@app.route("/search-ranking")
def search_ranking():
    q = request.args.get("q")  # None = first time the page is opened
    phrases = ASSIGNED if q is None else [q.strip()]
    results = [run_search(INDEX, TERMS, p) for p in phrases]
    return render_template(
        "search_ranking.html",
        page="search_ranking",
        stats=DATA["stats"],
        q=q,
        assigned=ASSIGNED,
        results=results,
    )

@app.route("/boolean")
def boolean_logic():
    a = request.args.get("a", ASSIGNED[0]).strip()
    b = request.args.get("b", ASSIGNED[1]).strip()
    op = request.args.get("op", "AND")
    if op not in ("AND", "AND NOT"):
        op = "AND"
    return render_template(
        "boolean_logic.html",
        page="boolean",
        stats=DATA["stats"],
        r=run_boolean(INDEX, DATA["documents"], a, b, op),
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)