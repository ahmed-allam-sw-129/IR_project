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
