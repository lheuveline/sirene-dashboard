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
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var dom_1 = require("@bokehjs/core/dom");
var p = __importStar(require("@bokehjs/core/properties"));
var html_box_1 = require("@bokehjs/models/layouts/html_box");
var layout_1 = require("./layout");
var tooltips_1 = require("./tooltips");
var constants_1 = __importDefault(require("@luma.gl/constants"));
var deck = window.deck;
var mapboxgl = window.mapboxgl;
var loaders = window.loaders;
function extractClasses() {
    // Get classes for registration from standalone deck.gl
    var classesDict = {};
    var classes = Object.keys(deck).filter(function (x) { return x.charAt(0) === x.charAt(0).toUpperCase(); });
    for (var _i = 0, classes_1 = classes; _i < classes_1.length; _i++) {
        var cls = classes_1[_i];
        classesDict[cls] = deck[cls];
    }
    return classesDict;
}
var DeckGLPlotView = /** @class */ (function (_super) {
    __extends(DeckGLPlotView, _super);
    function DeckGLPlotView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    DeckGLPlotView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        var _a = this.model.properties, data = _a.data, mapbox_api_key = _a.mapbox_api_key, tooltip = _a.tooltip, layers = _a.layers, initialViewState = _a.initialViewState, data_sources = _a.data_sources;
        this.on_change([mapbox_api_key, tooltip], function () { return _this.render(); });
        this.on_change([data, initialViewState], function () { return _this.updateDeck(); });
        this.on_change([layers], function () { return _this._update_layers(); });
        this.on_change([data_sources], function () { return _this._connect_sources(true); });
        this._layer_map = {};
        this._connected = [];
        this._connect_sources();
    };
    DeckGLPlotView.prototype._update_layers = function () {
        this._layer_map = {};
        this._update_data(true);
    };
    DeckGLPlotView.prototype._connect_sources = function (render) {
        var _this = this;
        if (render === void 0) { render = false; }
        for (var _i = 0, _a = this.model.data_sources; _i < _a.length; _i++) {
            var cds = _a[_i];
            if (this._connected.indexOf(cds) < 0) {
                this.connect(cds.properties.data.change, function () { return _this._update_data(true); });
                this._connected.push(cds);
            }
        }
        this._update_data(render);
    };
    DeckGLPlotView.prototype.initialize = function () {
        _super.prototype.initialize.call(this);
        if (deck.JSONConverter) {
            var CSVLoader = loaders.CSVLoader, Tile3DLoader = loaders.Tile3DLoader;
            loaders.registerLoaders([Tile3DLoader, CSVLoader]);
            var jsonConverterConfiguration = {
                classes: extractClasses(),
                // Will be resolved as `<enum-name>.<enum-value>`
                enumerations: {
                    COORDINATE_SYSTEM: deck.COORDINATE_SYSTEM,
                    GL: constants_1.default
                },
                // Constants that should be resolved with the provided values by JSON converter
                constants: {
                    Tile3DLoader: Tile3DLoader
                }
            };
            this.jsonConverter = new deck.JSONConverter({
                configuration: jsonConverterConfiguration
            });
        }
    };
    DeckGLPlotView.prototype._update_data = function (render) {
        if (render === void 0) { render = true; }
        var n = 0;
        for (var _i = 0, _a = this.model.layers; _i < _a.length; _i++) {
            var layer = _a[_i];
            var cds = void 0;
            n += 1;
            if ((n - 1) in this._layer_map) {
                cds = this.model.data_sources[this._layer_map[n - 1]];
            }
            else if (typeof layer.data != "number")
                continue;
            else {
                this._layer_map[n - 1] = layer.data;
                cds = this.model.data_sources[layer.data];
            }
            var data = [];
            var columns = cds.columns();
            for (var i = 0; i < cds.data[columns[0]].length; i++) {
                var item = {};
                for (var _b = 0, columns_1 = columns; _b < columns_1.length; _b++) {
                    var column = columns_1[_b];
                    var shape = cds._shapes[column];
                    if ((shape !== undefined) && (shape.length > 1) && (typeof shape[0] == "number"))
                        item[column] = cds.data[column].slice(i * shape[1], i * shape[1] + shape[1]);
                    else
                        item[column] = cds.data[column][i];
                }
                data.push(item);
            }
            layer.data = data;
        }
        if (render)
            this.updateDeck();
    };
    DeckGLPlotView.prototype._on_click_event = function (event) {
        var clickState = {
            coordinate: event.coordinate,
            lngLat: event.lngLat,
            index: event.index
        };
        this.model.clickState = clickState;
    };
    DeckGLPlotView.prototype._on_hover_event = function (event) {
        var hoverState = {
            coordinate: event.coordinate,
            lngLat: event.lngLat,
            index: event.index
        };
        this.model.hoverState = hoverState;
    };
    DeckGLPlotView.prototype._on_viewState_event = function (event) {
        this.model.viewState = event.viewState;
    };
    DeckGLPlotView.prototype.getData = function () {
        var _this = this;
        var data = __assign(__assign({}, this.model.data), { layers: this.model.layers, initialViewState: this.model.initialViewState, onViewStateChange: function (event) { return _this._on_viewState_event(event); }, onClick: function (event) { return _this._on_click_event(event); }, onHover: function (event) { return _this._on_hover_event(event); } });
        return data;
    };
    DeckGLPlotView.prototype.updateDeck = function () {
        if (!this.deckGL) {
            this.render();
            return;
        }
        var data = this.getData();
        if (deck.updateDeck) {
            deck.updateDeck(data, this.deckGL);
        }
        else {
            var results = this.jsonConverter.convert(data);
            this.deckGL.setProps(results);
        }
    };
    DeckGLPlotView.prototype.createDeck = function (_a) {
        var mapboxApiKey = _a.mapboxApiKey, container = _a.container, jsonInput = _a.jsonInput, tooltip = _a.tooltip;
        var deckgl;
        try {
            var props = this.jsonConverter.convert(jsonInput);
            var getTooltip = tooltips_1.makeTooltip(tooltip);
            deckgl = new deck.DeckGL(__assign(__assign({}, props), { map: mapboxgl, mapboxApiAccessToken: mapboxApiKey, container: container,
                getTooltip: getTooltip }));
        }
        catch (err) {
            console.error(err);
        }
        return deckgl;
    };
    DeckGLPlotView.prototype.render = function () {
        _super.prototype.render.call(this);
        var container = dom_1.div({ class: "deckgl" });
        layout_1.set_size(container, this.model);
        var MAPBOX_API_KEY = this.model.mapbox_api_key;
        var tooltip = this.model.tooltip;
        var data = this.getData();
        if (deck.createDeck) {
            this.deckGL = deck.createDeck({
                mapboxApiKey: MAPBOX_API_KEY,
                container: container,
                jsonInput: data,
                tooltip: tooltip
            });
        }
        else {
            this.deckGL = this.createDeck({
                mapboxApiKey: MAPBOX_API_KEY,
                container: container,
                jsonInput: data,
                tooltip: tooltip
            });
        }
        this.el.appendChild(container);
    };
    DeckGLPlotView.__name__ = "DeckGLPlotView";
    return DeckGLPlotView;
}(layout_1.PanelHTMLBoxView));
exports.DeckGLPlotView = DeckGLPlotView;
var DeckGLPlot = /** @class */ (function (_super) {
    __extends(DeckGLPlot, _super);
    function DeckGLPlot(attrs) {
        return _super.call(this, attrs) || this;
    }
    DeckGLPlot.init_DeckGLPlot = function () {
        this.prototype.default_view = DeckGLPlotView;
        this.define({
            data: [p.Any],
            data_sources: [p.Array, []],
            clickState: [p.Any],
            hoverState: [p.Any],
            initialViewState: [p.Any],
            layers: [p.Array, []],
            mapbox_api_key: [p.String],
            tooltip: [p.Any],
            viewState: [p.Any],
        });
        this.override({
            height: 400,
            width: 600
        });
    };
    DeckGLPlot.__name__ = "DeckGLPlot";
    DeckGLPlot.__module__ = "panel.models.deckgl";
    return DeckGLPlot;
}(html_box_1.HTMLBox));
exports.DeckGLPlot = DeckGLPlot;
DeckGLPlot.init_DeckGLPlot();
//# sourceMappingURL=deckgl.js.map