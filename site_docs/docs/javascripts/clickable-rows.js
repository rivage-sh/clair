/* Each row of a table in a .clickable-rows wrapper jumps to the section that
   the first link of the row points to. A click on the link keeps its own
   behaviour. */
function activateClickableRows() {
  document.querySelectorAll(".clickable-rows tbody tr").forEach(function (row) {
    const link = row.querySelector('a[href^="#"]');
    if (!link) {
      return;
    }
    row.addEventListener("click", function (event) {
      if (event.target.closest("a")) {
        return;
      }
      link.click();
    });
  });
}

if (typeof document$ !== "undefined") {
  document$.subscribe(activateClickableRows);
} else {
  document.addEventListener("DOMContentLoaded", activateClickableRows);
}
