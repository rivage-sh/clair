// app.js -- Application logic for clair docs

(function () {
    "use strict";

    var catalog = null;
    var selectedId = null;
    var selectFilterIds = null; // null = no filter; Set = active --select filter

    // ─── Bootstrap ────────────────────────────────────────────────────
    document.addEventListener("DOMContentLoaded", async function () {
        try {
            var response = await fetch("/api/catalog.json");
            catalog = await response.json();
        } catch (err) {
            document.getElementById("graph-container").innerHTML =
                '<div class="graph-empty">Failed to load catalog data.</div>';
            return;
        }

        // Set project name in header
        var projectNameEl = document.querySelector(".logo .project-name");
        if (projectNameEl) {
            projectNameEl.textContent = catalog.project_name;
        }

        // Update page title
        document.title = "clair docs -- " + catalog.project_name;

        // Render sidebar
        renderSidebar(catalog.trouves);
        updateSidebarCount();

        // Initialize graph
        ClairdocsGraph.init("graph-container", catalog, onNodeSelect);

        // Wire interactions
        wireSearch();
        wireSelectFilter();
        wireResizeHandles();
    });

    // ─── Sidebar Rendering ────────────────────────────────────────────
    function renderSidebar(trouves) {
        var tree = document.getElementById("sidebar-tree");
        tree.innerHTML = "";

        // Group by database > schema
        var groups = {};
        var fullNames = Object.keys(trouves).sort();

        for (var i = 0; i < fullNames.length; i++) {
            var fn = fullNames[i];
            var parts = fn.split(".");
            var dbName = parts[0] || fn;
            var schemaName = parts[1] || "";
            var tableName = parts[2] || parts[parts.length - 1];

            if (!groups[dbName]) groups[dbName] = {};
            if (!groups[dbName][schemaName]) groups[dbName][schemaName] = [];
            groups[dbName][schemaName].push({
                fullName: fn,
                tableName: tableName,
                trouve: trouves[fn],
            });
        }

        // Render groups
        var databases = Object.keys(groups).sort();
        for (var d = 0; d < databases.length; d++) {
            var dbName = databases[d];
            var dbGroup = document.createElement("div");
            dbGroup.className = "sidebar-group";
            dbGroup.setAttribute("data-database", dbName);

            var dbHeader = document.createElement("div");
            dbHeader.className = "group-header";
            dbHeader.innerHTML =
                '<span class="caret open"></span> ' + escapeHtml(dbName);
            dbHeader.addEventListener("click", toggleGroup);
            dbGroup.appendChild(dbHeader);

            var dbChildren = document.createElement("div");
            dbChildren.className = "group-children";

            var schemas = Object.keys(groups[dbName]).sort();
            for (var s = 0; s < schemas.length; s++) {
                var schemaName = schemas[s];
                var schemaGroup = document.createElement("div");
                schemaGroup.className = "sidebar-subgroup";
                schemaGroup.setAttribute("data-schema", schemaName);

                var schemaHeader = document.createElement("div");
                schemaHeader.className = "subgroup-header";
                schemaHeader.innerHTML =
                    '<span class="caret open"></span> ' +
                    escapeHtml(schemaName);
                schemaHeader.addEventListener("click", toggleGroup);
                schemaGroup.appendChild(schemaHeader);

                var schemaChildren = document.createElement("div");
                schemaChildren.className = "group-children";

                var items = groups[dbName][schemaName];
                for (var t = 0; t < items.length; t++) {
                    var item = items[t];
                    var el = document.createElement("div");
                    el.className = "sidebar-item";
                    el.setAttribute("data-full-name", item.fullName);
                    el.setAttribute("data-type", item.trouve.type);
                    el.setAttribute(
                        "data-docs",
                        (item.trouve.docs || "").toLowerCase()
                    );

                    // Column names for search
                    var colNames = (item.trouve.columns || [])
                        .map(function (c) {
                            return c.name;
                        })
                        .join(",")
                        .toLowerCase();
                    el.setAttribute("data-columns", colNames);

                    el.innerHTML =
                        '<span class="item-type-dot type-' +
                        escapeHtml(item.trouve.type) +
                        '"></span>' +
                        escapeHtml(item.tableName);

                    el.addEventListener("click", onSidebarItemClick);
                    schemaChildren.appendChild(el);
                }

                schemaGroup.appendChild(schemaChildren);
                dbChildren.appendChild(schemaGroup);
            }

            dbGroup.appendChild(dbChildren);
            tree.appendChild(dbGroup);
        }
    }

    function toggleGroup(event) {
        var header = event.currentTarget;
        var caret = header.querySelector(".caret");
        var children = header.nextElementSibling;
        if (!children) return;

        if (children.classList.contains("collapsed")) {
            children.classList.remove("collapsed");
            if (caret) caret.classList.add("open");
        } else {
            children.classList.add("collapsed");
            if (caret) caret.classList.remove("open");
        }
    }

    function onSidebarItemClick(event) {
        var el = event.currentTarget;
        var fullName = el.getAttribute("data-full-name");
        if (fullName) {
            ClairdocsGraph.selectNode(fullName);
            ClairdocsGraph.panToNode(fullName);
        }
    }

    function highlightSidebarItem(fullName) {
        var items = document.querySelectorAll(".sidebar-item");
        for (var i = 0; i < items.length; i++) {
            if (items[i].getAttribute("data-full-name") === fullName) {
                items[i].classList.add("active");
                // Scroll into view
                items[i].scrollIntoView({ block: "nearest", behavior: "smooth" });
            } else {
                items[i].classList.remove("active");
            }
        }
    }

    function updateSidebarCount() {
        var countEl = document.getElementById("sidebar-count");
        if (!countEl || !catalog) return;
        var total = Object.keys(catalog.trouves).length;
        var visible = document.querySelectorAll(
            '.sidebar-item:not([style*="display: none"])'
        ).length;
        if (visible === total) {
            countEl.textContent = total + " trouves";
        } else {
            countEl.textContent = visible + " of " + total + " trouves";
        }
    }

    // ─── Node Selection (called from graph or sidebar) ────────────────
    function onNodeSelect(fullName) {
        if (fullName === null) {
            selectedId = null;
            hideDetail();
            highlightSidebarItem(null);
            return;
        }
        selectedId = fullName;
        showDetail(catalog.trouves[fullName], fullName);
        highlightSidebarItem(fullName);
    }

    // ─── Detail Panel ─────────────────────────────────────────────────
    function showDetail(trouveData, fullName) {
        var panel = document.getElementById("detail-panel");
        var content = document.getElementById("detail-content");
        content.innerHTML = "";
        panel.classList.add("open");

        // Header
        var header = document.createElement("div");
        header.className = "detail-header";

        var closeBtn = document.createElement("button");
        closeBtn.className = "detail-close";
        closeBtn.innerHTML = "&times;";
        closeBtn.addEventListener("click", function () {
            ClairdocsGraph.clearSelection();
        });
        header.appendChild(closeBtn);

        var badge = document.createElement("div");
        badge.className = "type-badge type-" + trouveData.type;
        badge.textContent = trouveData.type.toUpperCase();
        header.appendChild(badge);

        var h2 = document.createElement("h2");
        h2.textContent = fullName;
        header.appendChild(h2);

        var docsP = document.createElement("p");
        if (trouveData.docs && trouveData.docs.trim()) {
            docsP.className = "docs";
            docsP.textContent = trouveData.docs;
        } else {
            docsP.className = "docs empty";
            docsP.textContent = "No description.";
        }
        header.appendChild(docsP);
        content.appendChild(header);

        // Columns section
        renderColumnsSection(content, trouveData.columns || [], trouveData.column_inference);

        // SQL / df_fn section (TABLE and VIEW only)
        if (trouveData.type !== "source" && trouveData.compiled) {
            renderSqlSection(content, trouveData.compiled.resolved_sql);
            renderDfFnSection(content, trouveData.compiled.resolved_df_fn);
        }

        // Tests section
        renderTestsSection(content, trouveData.tests || []);

        // Run Config section (TABLE only)
        if (trouveData.type === "table") {
            renderRunConfigSection(content, trouveData.run_config);
        }

        // Lineage section
        renderLineageSection(content, fullName);

        // File path section
        if (trouveData.compiled && trouveData.compiled.file_path) {
            renderFileSection(content, trouveData.compiled.file_path);
        }
    }

    function hideDetail() {
        var panel = document.getElementById("detail-panel");
        var content = document.getElementById("detail-content");
        panel.classList.remove("open");
        content.innerHTML = "";
    }

    // ─── Detail Sections ──────────────────────────────────────────────
    function renderColumnsSection(panel, columns, columnInference) {
        var section = createSection("Columns");

        // Determine which columns and status to display
        var inference = columnInference || {};
        var displayColumns = inference.columns || columns || [];
        var status = inference.status || (displayColumns.length > 0 ? "declared" : null);

        // Show a status badge for non-declared columns
        if (status && status !== "declared" && inference.message) {
            var notice = document.createElement("div");
            notice.className = "column-inference-notice status-" + status;
            notice.textContent = inference.message;
            section.appendChild(notice);
        }

        if (displayColumns.length === 0) {
            if (!inference.message) {
                var empty = document.createElement("p");
                empty.className = "empty-state";
                empty.textContent = "No columns defined.";
                section.appendChild(empty);
            }
        } else {
            var table = document.createElement("table");
            table.className = "columns-table";

            // For inferred columns, skip Type/Nullable/Description since they're unknown
            var isInferred = status === "inferred";

            var thead = document.createElement("thead");
            if (isInferred) {
                thead.innerHTML = "<tr><th>Name</th></tr>";
            } else {
                thead.innerHTML =
                    "<tr><th>Name</th><th>Type</th><th>Nullable</th><th>Description</th></tr>";
            }
            table.appendChild(thead);

            var tbody = document.createElement("tbody");
            for (var i = 0; i < displayColumns.length; i++) {
                var col = displayColumns[i];
                var tr = document.createElement("tr");
                if (isInferred) {
                    tr.innerHTML =
                        '<td class="col-name">' +
                        escapeHtml(col.name) +
                        "</td>";
                } else {
                    tr.innerHTML =
                        '<td class="col-name">' +
                        escapeHtml(col.name) +
                        "</td>" +
                        '<td class="col-type">' +
                        escapeHtml(col.type || "--") +
                        "</td>" +
                        '<td class="col-nullable">' +
                        (col.nullable !== false ? "Yes" : "No") +
                        "</td>" +
                        '<td class="col-docs">' +
                        escapeHtml(col.docs || "") +
                        "</td>";
                }
                tbody.appendChild(tr);
            }
            table.appendChild(tbody);
            section.appendChild(table);
        }

        panel.appendChild(section);
    }

    function renderSqlSection(panel, sql) {
        if (!sql) return;

        var section = createSection("SQL");
        var pre = document.createElement("pre");
        pre.className = "sql-block";
        var code = document.createElement("code");
        code.textContent = sql;
        pre.appendChild(code);
        section.appendChild(pre);
        panel.appendChild(section);
    }

    function renderDfFnSection(panel, dfFn) {
        if (!dfFn) return;

        var section = createSection("Python");
        var pre = document.createElement("pre");
        pre.className = "sql-block";
        var code = document.createElement("code");
        code.textContent = dfFn;
        pre.appendChild(code);
        section.appendChild(pre);
        panel.appendChild(section);
    }

    function renderTestsSection(panel, tests) {
        var section = createSection("Tests");

        if (tests.length === 0) {
            var empty = document.createElement("p");
            empty.className = "empty-state";
            empty.textContent = "No tests defined.";
            section.appendChild(empty);
        } else {
            var ul = document.createElement("ul");
            ul.className = "tests-list";

            for (var i = 0; i < tests.length; i++) {
                var test = tests[i];
                var li = document.createElement("li");

                var typeSpan = document.createElement("span");
                typeSpan.className = "test-type";
                typeSpan.textContent = test.type;
                li.appendChild(typeSpan);

                var paramsSpan = document.createElement("span");
                paramsSpan.className = "test-params";
                paramsSpan.textContent = formatTestParams(test);
                li.appendChild(paramsSpan);

                ul.appendChild(li);
            }
            section.appendChild(ul);
        }

        panel.appendChild(section);
    }

    function formatTestParams(test) {
        var params = [];
        if (test.column) params.push("column: " + test.column);
        if (test.columns) params.push("columns: " + test.columns.join(", "));
        if (test.min_rows !== undefined && test.min_rows !== null)
            params.push("min_rows: " + test.min_rows);
        if (test.max_rows !== undefined && test.max_rows !== null)
            params.push("max_rows: " + test.max_rows);
        return params.length ? params.join(", ") : "";
    }

    function renderRunConfigSection(panel, runConfig) {
        if (!runConfig) return;

        var section = createSection("Run Config");
        var dl = document.createElement("dl");
        dl.className = "config-dl";

        addConfigEntry(dl, "Run Mode", runConfig.run_mode);
        addConfigEntry(dl, "Incremental Mode", runConfig.incremental_mode);
        addConfigEntry(
            dl,
            "Primary Key Columns",
            runConfig.primary_key_columns ? runConfig.primary_key_columns.join(", ") : null
        );

        section.appendChild(dl);
        panel.appendChild(section);
    }

    function addConfigEntry(dl, label, value) {
        var dt = document.createElement("dt");
        dt.textContent = label;
        dl.appendChild(dt);

        var dd = document.createElement("dd");
        if (value === null || value === undefined) {
            dd.textContent = "--";
            dd.className = "null-value";
        } else {
            dd.textContent = value;
        }
        dl.appendChild(dd);
    }

    function renderLineageSection(panel, fullName) {
        var section = createSection("Lineage");

        var upstream = ClairdocsGraph.getDirectPredecessors(fullName);
        var downstream = ClairdocsGraph.getDirectSuccessors(fullName);

        // Upstream
        var upLabel = document.createElement("h3");
        upLabel.textContent = "Upstream";
        upLabel.style.marginTop = "14px";
        upLabel.style.marginBottom = "4px";
        section.appendChild(upLabel);

        if (upstream.length === 0) {
            var emptyUp = document.createElement("p");
            emptyUp.className = "empty-state";
            emptyUp.textContent = "None";
            section.appendChild(emptyUp);
        } else {
            var upNodes = document.createElement("div");
            upNodes.className = "lineage-list-nodes";
            var ulUp = document.createElement("ul");
            ulUp.className = "lineage-list";
            for (var i = 0; i < upstream.length; i++) {
                var li = document.createElement("li");
                var a = document.createElement("span");
                a.className = "lineage-link";
                a.setAttribute("data-trouve", upstream[i]);
                a.textContent = upstream[i];
                a.addEventListener("click", onLineageLinkClick);
                li.appendChild(a);
                ulUp.appendChild(li);
            }
            upNodes.appendChild(ulUp);
            section.appendChild(upNodes);
        }

        // Downstream
        var downLabel = document.createElement("h3");
        downLabel.textContent = "Downstream";
        downLabel.style.marginTop = "20px";
        downLabel.style.marginBottom = "4px";
        section.appendChild(downLabel);

        if (downstream.length === 0) {
            var emptyDown = document.createElement("p");
            emptyDown.className = "empty-state";
            emptyDown.textContent = "None";
            section.appendChild(emptyDown);
        } else {
            var downNodes = document.createElement("div");
            downNodes.className = "lineage-list-nodes";
            var ulDown = document.createElement("ul");
            ulDown.className = "lineage-list";
            for (var j = 0; j < downstream.length; j++) {
                var liDown = document.createElement("li");
                var aDown = document.createElement("span");
                aDown.className = "lineage-link";
                aDown.setAttribute("data-trouve", downstream[j]);
                aDown.textContent = downstream[j];
                aDown.addEventListener("click", onLineageLinkClick);
                liDown.appendChild(aDown);
                ulDown.appendChild(liDown);
            }
            downNodes.appendChild(ulDown);
            section.appendChild(downNodes);
        }

        panel.appendChild(section);
    }

    function renderFileSection(panel, filePath) {
        var section = createSection("File");
        var code = document.createElement("code");
        code.className = "file-path";
        // Convert PosixPath-style or WindowsPath to string
        code.textContent = String(filePath);
        section.appendChild(code);
        panel.appendChild(section);
    }

    function onLineageLinkClick(event) {
        var fullName = event.currentTarget.getAttribute("data-trouve");
        if (fullName) {
            ClairdocsGraph.selectNode(fullName);
            ClairdocsGraph.panToNode(fullName);
        }
    }

    // ─── Search ───────────────────────────────────────────────────────
    function wireSearch() {
        var searchInput = document.getElementById("search");
        if (!searchInput) return;

        searchInput.addEventListener("input", function () {
            filterSidebar();
        });
    }

    function filterSidebar() {
        var searchInput = document.getElementById("search");
        var query = (searchInput ? searchInput.value : "").toLowerCase();

        // Filter items
        var items = document.querySelectorAll(".sidebar-item");
        for (var i = 0; i < items.length; i++) {
            var el = items[i];
            var fullName = (el.getAttribute("data-full-name") || "").toLowerCase();
            var docs = el.getAttribute("data-docs") || "";
            var columns = el.getAttribute("data-columns") || "";

            var matchesSearch =
                !query ||
                fullName.indexOf(query) !== -1 ||
                docs.indexOf(query) !== -1 ||
                columns.indexOf(query) !== -1;

            var rawFullName = el.getAttribute("data-full-name") || "";
            var matchesSelect = !selectFilterIds || selectFilterIds.has(rawFullName);

            el.style.display = matchesSearch && matchesSelect ? "" : "none";
        }

        // Update group visibility
        updateGroupVisibility();
        updateSidebarCount();
    }

    function updateGroupVisibility() {
        // Hide subgroups where all children are hidden
        var subgroups = document.querySelectorAll(".sidebar-subgroup");
        for (var i = 0; i < subgroups.length; i++) {
            var sub = subgroups[i];
            var visibleItems = sub.querySelectorAll(
                '.sidebar-item:not([style*="display: none"])'
            );
            sub.style.display = visibleItems.length > 0 ? "" : "none";
        }

        // Hide groups where all subgroups are hidden
        var groups = document.querySelectorAll(".sidebar-group");
        for (var j = 0; j < groups.length; j++) {
            var grp = groups[j];
            var visibleSubs = grp.querySelectorAll(
                '.sidebar-subgroup:not([style*="display: none"])'
            );
            grp.style.display = visibleSubs.length > 0 ? "" : "none";
        }
    }

    // ─── Filters ───────────────────────────────────────────────────────
    function applyFilters() {
        ClairdocsGraph.applyVisibility(selectFilterIds);
        filterSidebar();
    }

    // ─── Select Filter (--select syntax) ──────────────────────────────
    function wireSelectFilter() {
        var input = document.getElementById("select-filter");
        var clearBtn = document.getElementById("select-filter-clear");
        if (!input) return;

        input.addEventListener("keydown", function (e) {
            if (e.key === "Enter") applySelectFilter(input.value.trim());
        });

        input.addEventListener("input", function () {
            if (clearBtn) {
                clearBtn.classList.toggle("visible", input.value.length > 0);
            }
        });

        if (clearBtn) {
            clearBtn.addEventListener("click", function () {
                input.value = "";
                clearBtn.classList.remove("visible");
                applySelectFilter("");
            });
        }
    }

    function applySelectFilter(expr) {
        if (!expr) {
            selectFilterIds = null;
        } else {
            var parsed = parseSelectExpr(expr);
            selectFilterIds = parsed ? computeSelectNodes(parsed) : new Set();
        }
        applyFilters();
    }

    // Parse a --select expression into {upstream, pattern, downstream}.
    // upstream/downstream are hop counts (Infinity = all, 0 = none).
    // Pattern may contain * as a glob wildcard.
    function parseSelectExpr(expr) {
        if (expr === "*") return { upstream: Infinity, pattern: "*", downstream: Infinity };
        var m = expr.match(/^(\+(\d+)?)?([^+\s]+?)(\+(\d+)?)?$/);
        if (!m || !m[3]) return null;
        return {
            upstream: m[1] ? (m[2] ? parseInt(m[2], 10) : Infinity) : 0,
            pattern: m[3],
            downstream: m[4] ? (m[5] ? parseInt(m[5], 10) : Infinity) : 0,
        };
    }

    function matchPattern(fullName, pattern) {
        if (pattern === "*") return true;
        if (pattern.indexOf("*") === -1) return fullName === pattern;
        var re = new RegExp(
            "^" +
            pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*") +
            "$"
        );
        return re.test(fullName);
    }

    function computeSelectNodes(parsed) {
        var allIds = Object.keys(catalog.trouves);
        var seeds = allIds.filter(function (fn) { return matchPattern(fn, parsed.pattern); });
        if (seeds.length === 0) return new Set();

        var visible = new Set(seeds);
        var edges = catalog.edges || [];

        // Build adjacency maps
        var predecessors = {};
        var successors = {};
        allIds.forEach(function (fn) { predecessors[fn] = []; successors[fn] = []; });
        edges.forEach(function (e) {
            if (successors[e.source]) successors[e.source].push(e.target);
            if (predecessors[e.target]) predecessors[e.target].push(e.source);
        });

        // BFS upstream
        if (parsed.upstream > 0) {
            var upQueue = seeds.map(function (s) { return [s, 0]; });
            while (upQueue.length) {
                var upItem = upQueue.shift();
                var upNode = upItem[0], upHops = upItem[1];
                if (upHops >= parsed.upstream) continue;
                (predecessors[upNode] || []).forEach(function (pred) {
                    if (!visible.has(pred)) {
                        visible.add(pred);
                        upQueue.push([pred, upHops + 1]);
                    }
                });
            }
        }

        // BFS downstream
        if (parsed.downstream > 0) {
            var downQueue = seeds.map(function (s) { return [s, 0]; });
            while (downQueue.length) {
                var downItem = downQueue.shift();
                var downNode = downItem[0], downHops = downItem[1];
                if (downHops >= parsed.downstream) continue;
                (successors[downNode] || []).forEach(function (succ) {
                    if (!visible.has(succ)) {
                        visible.add(succ);
                        downQueue.push([succ, downHops + 1]);
                    }
                });
            }
        }

        return visible;
    }

    // ─── Resize Handles ───────────────────────────────────────────────
    function wireResizeHandles() {
        var sidebarHandle = document.getElementById("sidebar-resize-handle");
        var sidebar = document.getElementById("sidebar");

        if (sidebarHandle && sidebar) {
            sidebarHandle.addEventListener("mousedown", function (e) {
                e.preventDefault();
                var startX = e.clientX;
                var startWidth = sidebar.offsetWidth;
                sidebarHandle.classList.add("dragging");

                function onMouseMove(e) {
                    var newWidth = Math.max(150, Math.min(500, startWidth + e.clientX - startX));
                    sidebar.style.width = newWidth + "px";
                    sidebar.style.minWidth = newWidth + "px";
                }

                function onMouseUp() {
                    sidebarHandle.classList.remove("dragging");
                    document.removeEventListener("mousemove", onMouseMove);
                    document.removeEventListener("mouseup", onMouseUp);
                }

                document.addEventListener("mousemove", onMouseMove);
                document.addEventListener("mouseup", onMouseUp);
            });
        }

        var detailHandle = document.getElementById("detail-resize-handle");
        var detailPanel = document.getElementById("detail-panel");

        if (detailHandle && detailPanel) {
            detailHandle.addEventListener("mousedown", function (e) {
                e.preventDefault();
                var startX = e.clientX;
                var startWidth = detailPanel.offsetWidth;
                detailHandle.classList.add("dragging");

                function onMouseMove(e) {
                    var newWidth = Math.max(200, Math.min(700, startWidth - (e.clientX - startX)));
                    detailPanel.style.width = newWidth + "px";
                }

                function onMouseUp() {
                    detailHandle.classList.remove("dragging");
                    document.removeEventListener("mousemove", onMouseMove);
                    document.removeEventListener("mouseup", onMouseUp);
                }

                document.addEventListener("mousemove", onMouseMove);
                document.addEventListener("mouseup", onMouseUp);
            });
        }
    }

    // ─── Utilities ────────────────────────────────────────────────────
    function createSection(title) {
        var section = document.createElement("div");
        section.className = "detail-section";

        var h3 = document.createElement("h3");
        h3.textContent = title;
        section.appendChild(h3);

        return section;
    }

    function escapeHtml(str) {
        if (!str) return "";
        return str
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }
})();
