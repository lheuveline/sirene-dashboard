"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
var p = __importStar(require("@bokehjs/core/properties"));
var object_1 = require("@bokehjs/core/util/object");
var eq_1 = require("@bokehjs/core/util/eq");
var html_box_1 = require("@bokehjs/models/layouts/html_box");
var debounce_1 = require("debounce");
var util_1 = require("./util");
var layout_1 = require("./layout");
var Plotly = window.Plotly;
var filterEventData = function (gd, eventData, event) {
    // Ported from dash-core-components/src/components/Graph.react.js
    var filteredEventData = Array.isArray(eventData) ? [] : {};
    if (event === "click" || event === "hover" || event === "selected") {
        var points = [];
        if (eventData === undefined || eventData === null) {
            return null;
        }
        /*
         * remove `data`, `layout`, `xaxis`, etc
         * objects from the event data since they're so big
         * and cause JSON stringify ciricular structure errors.
         *
         * also, pull down the `customdata` point from the data array
         * into the event object
         */
        var data = gd.data;
        for (var i = 0; i < eventData.points.length; i++) {
            var fullPoint = eventData.points[i];
            var pointData = {};
            for (var property in fullPoint) {
                var val = fullPoint[property];
                if (fullPoint.hasOwnProperty(property) &&
                    !Array.isArray(val) && !util_1.isPlainObject(val)) {
                    pointData[property] = val;
                }
            }
            if (fullPoint !== undefined && fullPoint !== null) {
                if (fullPoint.hasOwnProperty("curveNumber") &&
                    fullPoint.hasOwnProperty("pointNumber") &&
                    data[fullPoint["curveNumber"]].hasOwnProperty("customdata")) {
                    pointData["customdata"] =
                        data[fullPoint["curveNumber"]].customdata[fullPoint["pointNumber"]];
                }
                // specific to histogram. see https://github.com/plotly/plotly.js/pull/2113/
                if (fullPoint.hasOwnProperty('pointNumbers')) {
                    pointData["pointNumbers"] = fullPoint.pointNumbers;
                }
            }
            points[i] = pointData;
        }
        filteredEventData["points"] = points;
    }
    else if (event === 'relayout' || event === 'restyle') {
        /*
         * relayout shouldn't include any big objects
         * it will usually just contain the ranges of the axes like
         * "xaxis.range[0]": 0.7715822247381828,
         * "xaxis.range[1]": 3.0095292008680063`
         */
        for (var property in eventData) {
            if (eventData.hasOwnProperty(property)) {
                filteredEventData[property] = eventData[property];
            }
        }
    }
    if (eventData.hasOwnProperty('range')) {
        filteredEventData["range"] = eventData["range"];
    }
    if (eventData.hasOwnProperty('lassoPoints')) {
        filteredEventData["lassoPoints"] = eventData["lassoPoints"];
    }
    return filteredEventData;
};
var PlotlyPlotView = /** @class */ (function (_super) {
    __extends(PlotlyPlotView, _super);
    function PlotlyPlotView() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this._settingViewport = false;
        _this._plotInitialized = false;
        _this._reacting = false;
        _this._relayouting = false;
        _this._end_relayouting = debounce_1.debounce(function () {
            _this._relayouting = false;
        }, 2000, false);
        return _this;
    }
    PlotlyPlotView.prototype.connect_signals = function () {
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.viewport_update_policy.change, this._updateSetViewportFunction);
        this.connect(this.model.properties.viewport_update_throttle.change, this._updateSetViewportFunction);
        this.connect(this.model.properties._render_count.change, this.render);
        this.connect(this.model.properties.viewport.change, this._updateViewportFromProperty);
    };
    PlotlyPlotView.prototype.render = function () {
        var _this = this;
        _super.prototype.render.call(this);
        if (!window.Plotly) {
            return;
        }
        var data = [];
        for (var i = 0; i < this.model.data.length; i++) {
            data.push(this._get_trace(i, false));
        }
        var newLayout = util_1.deepCopy(this.model.layout);
        if (this._relayouting) {
            var layout = this.el.layout;
            // For each xaxis* and yaxis* property of layout, if the value has a 'range'
            // property then use this in newLayout
            Object.keys(layout).reduce(function (value, key) {
                if (key.slice(1, 5) === "axis" && 'range' in value) {
                    newLayout[key].range = value.range;
                }
            }, {});
        }
        this._reacting = true;
        Plotly.react(this.el, data, newLayout, this.model.config).then(function () {
            _this._updateSetViewportFunction();
            _this._updateViewportProperty();
            if (!_this._plotInitialized) {
                // Install callbacks
                //  - plotly_relayout
                (_this.el).on('plotly_relayout', function (eventData) {
                    if (eventData['_update_from_property'] !== true) {
                        _this.model.relayout_data = filterEventData(_this.el, eventData, 'relayout');
                        _this._updateViewportProperty();
                        _this._end_relayouting();
                    }
                });
                //  - plotly_relayouting
                (_this.el).on('plotly_relayouting', function () {
                    if (_this.model.viewport_update_policy !== 'mouseup') {
                        _this._relayouting = true;
                        _this._updateViewportProperty();
                    }
                });
                //  - plotly_restyle
                (_this.el).on('plotly_restyle', function (eventData) {
                    _this.model.restyle_data = filterEventData(_this.el, eventData, 'restyle');
                    _this._updateViewportProperty();
                });
                //  - plotly_click
                (_this.el).on('plotly_click', function (eventData) {
                    _this.model.click_data = filterEventData(_this.el, eventData, 'click');
                });
                //  - plotly_hover
                (_this.el).on('plotly_hover', function (eventData) {
                    _this.model.hover_data = filterEventData(_this.el, eventData, 'hover');
                });
                //  - plotly_selected
                (_this.el).on('plotly_selected', function (eventData) {
                    _this.model.selected_data = filterEventData(_this.el, eventData, 'selected');
                });
                //  - plotly_clickannotation
                (_this.el).on('plotly_clickannotation', function (eventData) {
                    delete eventData["event"];
                    delete eventData["fullAnnotation"];
                    _this.model.clickannotation_data = eventData;
                });
                //  - plotly_deselect
                (_this.el).on('plotly_deselect', function () {
                    _this.model.selected_data = null;
                });
                //  - plotly_unhover
                (_this.el).on('plotly_unhover', function () {
                    _this.model.hover_data = null;
                });
            }
            _this._plotInitialized = true;
            _this._reacting = false;
        });
    };
    PlotlyPlotView.prototype._get_trace = function (index, update) {
        var trace = object_1.clone(this.model.data[index]);
        var cds = this.model.data_sources[index];
        for (var _i = 0, _a = cds.columns(); _i < _a.length; _i++) {
            var column = _a[_i];
            var shape = cds._shapes[column][0];
            var array = cds.get_array(column)[0];
            if (shape.length > 1) {
                var arrays = [];
                for (var s = 0; s < shape[0]; s++) {
                    arrays.push(array.slice(s * shape[1], (s + 1) * shape[1]));
                }
                array = arrays;
            }
            var prop_path = column.split(".");
            var prop = prop_path[prop_path.length - 1];
            var prop_parent = trace;
            for (var _b = 0, _c = prop_path.slice(0, -1); _b < _c.length; _b++) {
                var k = _c[_b];
                prop_parent = prop_parent[k];
            }
            if (update && prop_path.length == 1) {
                prop_parent[prop] = [array];
            }
            else {
                prop_parent[prop] = array;
            }
        }
        return trace;
    };
    PlotlyPlotView.prototype._updateViewportFromProperty = function () {
        var _this = this;
        if (!Plotly || this._settingViewport || this._reacting || !this.model.viewport) {
            return;
        }
        var fullLayout = this.el._fullLayout;
        // Call relayout if viewport differs from fullLayout
        Object.keys(this.model.viewport).reduce(function (value, key) {
            if (!eq_1.isEqual(util_1.get(fullLayout, key), value)) {
                var clonedViewport = util_1.deepCopy(_this.model.viewport);
                clonedViewport['_update_from_property'] = true;
                Plotly.relayout(_this.el, clonedViewport);
                return false;
            }
            else {
                return true;
            }
        }, {});
    };
    PlotlyPlotView.prototype._updateViewportProperty = function () {
        var fullLayout = this.el._fullLayout;
        var viewport = {};
        // Get range for all xaxis and yaxis properties
        for (var prop in fullLayout) {
            if (!fullLayout.hasOwnProperty(prop)) {
                continue;
            }
            var maybe_axis = prop.slice(0, 5);
            if (maybe_axis === 'xaxis' || maybe_axis === 'yaxis') {
                viewport[prop + '.range'] = util_1.deepCopy(fullLayout[prop].range);
            }
        }
        if (!eq_1.isEqual(viewport, this.model.viewport)) {
            this._setViewport(viewport);
        }
    };
    PlotlyPlotView.prototype._updateSetViewportFunction = function () {
        var _this = this;
        if (this.model.viewport_update_policy === "continuous" ||
            this.model.viewport_update_policy === "mouseup") {
            this._setViewport = function (viewport) {
                if (!_this._settingViewport) {
                    _this._settingViewport = true;
                    _this.model.viewport = viewport;
                    _this._settingViewport = false;
                }
            };
        }
        else {
            this._setViewport = util_1.throttle(function (viewport) {
                if (!_this._settingViewport) {
                    _this._settingViewport = true;
                    _this.model.viewport = viewport;
                    _this._settingViewport = false;
                }
            }, this.model.viewport_update_throttle);
        }
    };
    PlotlyPlotView.__name__ = "PlotlyPlotView";
    return PlotlyPlotView;
}(layout_1.PanelHTMLBoxView));
exports.PlotlyPlotView = PlotlyPlotView;
var PlotlyPlot = /** @class */ (function (_super) {
    __extends(PlotlyPlot, _super);
    function PlotlyPlot(attrs) {
        return _super.call(this, attrs) || this;
    }
    PlotlyPlot.init_PlotlyPlot = function () {
        this.prototype.default_view = PlotlyPlotView;
        this.define({
            data: [p.Array, []],
            layout: [p.Any, {}],
            config: [p.Any, {}],
            data_sources: [p.Array, []],
            relayout_data: [p.Any, {}],
            restyle_data: [p.Array, []],
            click_data: [p.Any, {}],
            hover_data: [p.Any, {}],
            clickannotation_data: [p.Any, {}],
            selected_data: [p.Any, {}],
            viewport: [p.Any, {}],
            viewport_update_policy: [p.String, "mouseup"],
            viewport_update_throttle: [p.Number, 200],
            _render_count: [p.Number, 0],
        });
    };
    PlotlyPlot.__name__ = "PlotlyPlot";
    PlotlyPlot.__module__ = "panel.models.plotly";
    return PlotlyPlot;
}(html_box_1.HTMLBox));
exports.PlotlyPlot = PlotlyPlot;
PlotlyPlot.init_PlotlyPlot();
//# sourceMappingURL=plotly.js.map