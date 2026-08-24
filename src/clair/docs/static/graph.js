// graph.js -- Cytoscape.js draws the graph of clair docs

var ClairdocsGraph = (function () {
    "use strict";

    var cy = null;
    var onSelectCallback = null;

    // ─── The Cytoscape stylesheet ─────────────────────────────────────────
    var STYLE = [
        // The default node.
        {
            selector: "node",
            style: {
                "label": "data(label)",
                "text-valign": "center",
                "text-halign": "center",
                "font-size": "13px",
                "font-family": "-apple-system, BlinkMacSystemFont, sans-serif",
                "background-color": "#4a90d9",
                "color": "#fff",
                "width": "label",
                "height": 36,
                "padding": "12px",
                "shape": "roundrectangle",
                "text-wrap": "none",
            },
        },
        // A SOURCE node: a gray rectangle with round corners.
        {
            selector: "node[type='source']",
            style: {
                "background-color": "#6b7280",
                "shape": "roundrectangle",
            },
        },
        // A TABLE node: a blue rectangle with round corners.
        {
            selector: "node[type='table']",
            style: {
                "background-color": "#4a90d9",
                "shape": "roundrectangle",
            },
        },
        // A VIEW node: a dashed border and a paler color.
        {
            selector: "node[type='view']",
            style: {
                "background-color": "#93c5fd",
                "color": "#1e3a5f",
                "border-style": "dashed",
                "border-width": 2,
                "border-color": "#4a90d9",
            },
        },
        // A node that has tests: a green border.
        {
            selector: "node[testCount > 0]",
            style: {
                "border-width": 2,
                "border-color": "#22c55e",
            },
        },
        // The default edge.
        {
            selector: "edge",
            style: {
                "width": 1.5,
                "line-color": "#cbd5e1",
                "target-arrow-color": "#cbd5e1",
                "target-arrow-shape": "triangle",
                "curve-style": "bezier",
                "arrow-scale": 0.8,
            },
        },
        // A node in the lineage, with a highlight.
        {
            selector: "node.highlighted",
            style: {
                "border-width": 3,
                "border-color": "#f59e0b",
                "background-opacity": 1,
            },
        },
        // The node that the user selected.
        {
            selector: "node.selected-node",
            style: {
                "border-width": 3,
                "border-color": "#ef4444",
            },
        },
        // An edge with a highlight.
        {
            selector: "edge.highlighted",
            style: {
                "line-color": "#f59e0b",
                "target-arrow-color": "#f59e0b",
                "width": 2.5,
            },
        },
        // An element that is not in the lineage, with a pale color.
        {
            selector: "node.dimmed",
            style: {
                "opacity": 0.2,
            },
        },
        {
            selector: "edge.dimmed",
            style: {
                "opacity": 0.15,
            },
        },
    ];

    // ─── The layout configuration ─────────────────────────────────────────
    var LAYOUT = {
        name: "dagre",
        rankDir: "LR",
        nodeSep: 40,
        rankSep: 80,
        edgeSep: 10,
        padding: 30,
        animate: false,
        fit: true,
    };

    // ─── Make the Cytoscape elements from the catalog ────────────────────────
    function buildElements(catalog) {
        var nodes = [];
        var trouves = catalog.trouves;
        var fullNames = Object.keys(trouves);

        for (var i = 0; i < fullNames.length; i++) {
            var fn = fullNames[i];
            var t = trouves[fn];
            // The compiled object holds physical_name. The key in the dict is the physical_name too.
            var parts = fn.split(".");
            var label = fn;
            var databaseName = parts.length >= 1 ? parts[0] : "";
            var schemaName = parts.length >= 2 ? parts[1] : "";

            nodes.push({
                group: "nodes",
                data: {
                    id: fn,
                    label: label,
                    fullName: fn,
                    type: t.type,
                    databaseName: databaseName,
                    schemaName: schemaName,
                    testCount: (t.tests || []).length,
                    hasDocs: !!(t.docs && t.docs.trim()),
                },
            });
        }

        var edges = [];
        var catalogEdges = catalog.edges || [];
        for (var j = 0; j < catalogEdges.length; j++) {
            var e = catalogEdges[j];
            edges.push({
                group: "edges",
                data: {
                    id: e.source + "->" + e.target,
                    source: e.source,
                    target: e.target,
                },
            });
        }

        return nodes.concat(edges);
    }

    // ─── The start of the graph ───────────────────────────────────────────────
    function init(containerId, catalog, onSelect) {
        onSelectCallback = onSelect;

        // Add the dagre layout.
        if (typeof cytoscapeDagre !== "undefined") {
            cytoscape.use(cytoscapeDagre);
        }

        var elements = buildElements(catalog);

        cy = cytoscape({
            container: document.getElementById(containerId),
            elements: elements,
            style: STYLE,
            layout: LAYOUT,
            minZoom: 0.2,
            maxZoom: 3,
            wheelSensitivity: 0.3,
        });

        // Connect the events.
        cy.on("tap", "node", function (e) {
            selectNode(e.target.id());
        });

        cy.on("tap", function (e) {
            if (e.target === cy) {
                clearSelection();
            }
        });
    }

    // ─── The selection and the lineage highlight ─────────────────────────────
    function selectNode(nodeId) {
        clearClasses();

        var node = cy.getElementById(nodeId);
        if (!node.length) return;

        node.addClass("selected-node");

        // Find the complete lineage with a breadth-first search.
        var upstream = {};
        var downstream = {};
        walkPredecessors(node, upstream);
        walkSuccessors(node, downstream);

        var lineageNodes = {};
        lineageNodes[nodeId] = true;
        var key;
        for (key in upstream) { lineageNodes[key] = true; }
        for (key in downstream) { lineageNodes[key] = true; }

        cy.elements().forEach(function (ele) {
            if (ele.isNode()) {
                if (lineageNodes[ele.id()]) {
                    if (ele.id() !== nodeId) ele.addClass("highlighted");
                } else {
                    ele.addClass("dimmed");
                }
            } else {
                // An edge gets a highlight if the lineage holds its two nodes.
                var sourceIn = !!lineageNodes[ele.source().id()];
                var targetIn = !!lineageNodes[ele.target().id()];
                if (sourceIn && targetIn) {
                    ele.addClass("highlighted");
                } else {
                    ele.addClass("dimmed");
                }
            }
        });

        if (onSelectCallback) onSelectCallback(nodeId);
    }

    function clearSelection() {
        clearClasses();
        if (onSelectCallback) onSelectCallback(null);
    }

    function clearClasses() {
        if (cy) {
            cy.elements().removeClass("selected-node highlighted dimmed");
        }
    }

    function walkPredecessors(node, visited) {
        node.incomers("node").forEach(function (n) {
            if (!visited[n.id()]) {
                visited[n.id()] = true;
                walkPredecessors(n, visited);
            }
        });
    }

    function walkSuccessors(node, visited) {
        node.outgoers("node").forEach(function (n) {
            if (!visited[n.id()]) {
                visited[n.id()] = true;
                walkSuccessors(n, visited);
            }
        });
    }

    function panToNode(nodeId) {
        if (!cy) return;
        var node = cy.getElementById(nodeId);
        if (node.length) {
            cy.animate({ center: { eles: node }, duration: 300 });
        }
    }

    function applyVisibility(selectIds) {
        if (!cy) return;
        cy.nodes().forEach(function (node) {
            node.style("display", !selectIds || selectIds.has(node.id()) ? "element" : "none");
        });
        cy.edges().forEach(function (edge) {
            var show =
                edge.source().style("display") !== "none" &&
                edge.target().style("display") !== "none";
            edge.style("display", show ? "element" : "none");
        });
        cy.elements(":visible").layout(LAYOUT).run();
    }

    function getDirectPredecessors(fullName) {
        if (!cy) return [];
        var node = cy.getElementById(fullName);
        if (!node.length) return [];
        var result = [];
        node.incomers("node").forEach(function (n) {
            result.push(n.id());
        });
        return result;
    }

    function getDirectSuccessors(fullName) {
        if (!cy) return [];
        var node = cy.getElementById(fullName);
        if (!node.length) return [];
        var result = [];
        node.outgoers("node").forEach(function (n) {
            result.push(n.id());
        });
        return result;
    }

    // ─── The public API ───────────────────────────────────────────────────
    return {
        init: init,
        selectNode: selectNode,
        clearSelection: clearSelection,
        panToNode: panToNode,
        applyVisibility: applyVisibility,
        getDirectPredecessors: getDirectPredecessors,
        getDirectSuccessors: getDirectSuccessors,
    };
})();
