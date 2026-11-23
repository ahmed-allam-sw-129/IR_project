// Dark / light mode
document.getElementById("theme-toggle").addEventListener("click", function () {
  var root = document.documentElement;
  var dark = root.dataset.theme === "dark";
  root.dataset.theme = dark ? "light" : "dark";
  localStorage.setItem("theme", dark ? "light" : "dark");
});

// Dataset page: click a row to show / hide its token positions
document.querySelectorAll(".doc-row").forEach(function (row) {
  row.addEventListener("click", function () {
    var detail = document.getElementById(row.dataset.target);
    detail.hidden = !detail.hidden;
    row.classList.toggle("open", !detail.hidden);
  });
});

// Positional Index page: filter the terms while typing
var termFilter = document.getElementById("term-filter");
if (termFilter) {
  termFilter.addEventListener("input", function () {
    var text = termFilter.value.trim().toLowerCase();
    var shown = 0;
    document.querySelectorAll(".index-row").forEach(function (row) {
      var match = row.dataset.term.indexOf(text) !== -1;
      row.hidden = !match;
      if (match) shown++;
    });
    document.getElementById("term-count").textContent = shown;
  });
}

// Term Analysis page: switch between the tables with the tabs
document.querySelectorAll(".tab").forEach(function (tab) {
  tab.addEventListener("click", function () {
    document.querySelectorAll(".tab").forEach(function (t) { t.classList.remove("active"); });
    document.querySelectorAll(".tab-panel").forEach(function (p) { p.hidden = true; });
    tab.classList.add("active");
    document.getElementById("panel-" + tab.dataset.tab).hidden = false;
  });
});
