[project_readme.md](https://github.com/user-attachments/files/35548015/project_readme.md)
# Information Retrieval (IR) System

A full-stack Information Retrieval (IR) engine built with Python, Apache Spark, and a custom web interface. This system demonstrates core IR concepts including positional indexing, TF-IDF weighting, cosine similarity ranking, term analysis, and Boolean retrieval logic across various document formats.

---

## 🚀 Key Features

* **Multi-Format Document Ingestion:** Supports `.txt`, `.json`, and `.csv` datasets powered by a Spark data loader.
* **Positional Indexing:** Maps terms to their exact document and word-level positions to enable phrase queries and proximity searches.
* **Term Analysis & TF-IDF:** Computes Term Frequency (TF), Inverse Document Frequency (IDF), and TF-IDF matrices to evaluate term importance across the corpus.
* **Ranked Search & Scoring:** Retrieves and ranks relevant documents using vector space models and similarity scoring.
* **Boolean Retrieval:** Evaluates complex logical queries using `AND`, `OR`, and `NOT` operators.
* **Interactive Web UI:** A clean, responsive dashboard built with Flask/Jinja templates to visualize datasets, indexes, term statistics, and search results.

---

## 👥 Team Contributions & Module Ownership

| Team Member | Assigned Modules | Core Files & Templates |
| :--- | :--- | :--- |
| **Mark Naiem** | Overview, Dataset & Document Management | `spark_loader.py`, `overview.html`, `dataset.html`, `docs/` |
| **Ahmed Hany** | Search & Ranking | `search.py`, `search_ranking.html` |
| **Yassin Mohamed** | Positional Indexing | `positional_index.py`, `positional_index.html` |
| **Abdallah Amr** | Term Analysis | `tf_idf.py`, `term_analysis.html` |
| **Yahia Abdallrhman** | Boolean Logic | `boolean_logic.py`, `boolean_logic.html` |

---

## 📂 Project Structure

```text
IR_project/
└── IR_project-final_project/
    ├── docs/                     # Primary text document corpus (d1.txt - d10.txt)
    ├── test_docs/                # Sample documents for unit testing (a.txt, b.txt, c.txt)
    ├── test_types/               # Multi-format test files (a.txt, more.json, notes.csv)
    ├── src/                      # Core Information Retrieval backend engines
    │   ├── boolean_logic.py      # Boolean query parser and evaluator
    │   ├── positional_index.py   # Positional inverted index builder
    │   ├── search.py             # Search execution and document ranking logic
    │   ├── spark_loader.py       # PySpark dataset ingestion and preprocessing
    │   └── tf_idf.py             # TF-IDF calculation and term weighting
    ├── web_ui/                   # Web application interface
    │   ├── static/               # Frontend assets
    │   │   ├── app.js            # Client-side interactivity
    │   │   └── style.css         # Custom styling
    │   ├── templates/            # HTML views
    │   │   ├── _icons.html       # SVG icon partials
    │   │   ├── base.html         # Base layout template
    │   │   ├── boolean_logic.html
    │   │   ├── dataset.html
    │   │   ├── overview.html
    │   │   ├── positional_index.html
    │   │   ├── search_ranking.html
    │   │   └── term_analysis.html
    │   └── app.py                # Main web server entry point
    └── README.md                 # Project documentation
```

---

## 🛠️ Tech Stack

* **Backend:** Python 3.x, Flask
* **Data Processing:** Apache Spark (`PySpark`)
* **Frontend:** HTML5, CSS3, Vanilla JavaScript, Jinja2

---

## ⚙️ Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/ahmed-allam-sw-129/IR_project.git
   cd IR_project/IR_project-final_project
   ```

2. **Create and activate a virtual environment (optional but recommended):**
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install required dependencies:**
   ```bash
   pip install flask pyspark
   ```
   *(Note: Ensure Java JDK 8 or 11+ is installed and `JAVA_HOME` is configured for PySpark).*

4. **Run the application:**
   ```bash
   python web_ui/app.py
   ```

5. **Access the Web UI:**
   Open your browser and navigate to `http://127.0.0.1:5000` (or the port displayed in your terminal).
