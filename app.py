from flask import Flask, render_template

from src.spark_loader import load_dataset
from src.positional_index import build_positional_index, index_for_page

app = Flask(
    __name__,
    template_folder="web_ui/templates",
    static_folder="web_ui/static",
)

# Spark runs once when the server starts; the pages reuse the result.
DATA = load_dataset()
INDEX = build_positional_index(DATA["documents"])  # term -> {doc: [positions]}


@app.route("/")
def overview():
    return render_template("overview.html", page="overview", stats=DATA["stats"])


@app.route("/dataset")
def dataset():
    return render_template(
        "dataset.html",
        page="dataset",
        stats=DATA["stats"],
        documents=DATA["documents"],
    )


@app.route("/positional-index")
def positional_index():
    return render_template(
        "positional_index.html",
        page="positional_index",
        stats=DATA["stats"],
        rows=index_for_page(INDEX),
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
