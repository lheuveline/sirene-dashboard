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
var html_box_1 = require("@bokehjs/models/layouts/html_box");
var VegaPlotView = /** @class */ (function (_super) {
    __extends(VegaPlotView, _super);
    function VegaPlotView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    VegaPlotView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.data.change, this._plot);
        this.connect(this.model.properties.data_sources.change, function () { return _this._connect_sources(); });
        this._connected = [];
        this._connect_sources();
    };
    VegaPlotView.prototype._connect_sources = function () {
        for (var ds in this.model.data_sources) {
            var cds = this.model.data_sources[ds];
            if (this._connected.indexOf(ds) < 0) {
                this.connect(cds.properties.data.change, this._plot);
                this._connected.push(ds);
            }
        }
    };
    VegaPlotView.prototype._fetch_datasets = function () {
        var datasets = {};
        for (var ds in this.model.data_sources) {
            var cds = this.model.data_sources[ds];
            var data = [];
            var columns = cds.columns();
            for (var i = 0; i < cds.data[columns[0]].length; i++) {
                var item = {};
                for (var _i = 0, columns_1 = columns; _i < columns_1.length; _i++) {
                    var column = columns_1[_i];
                    item[column] = cds.data[column][i];
                }
                data.push(item);
            }
            datasets[ds] = data;
        }
        return datasets;
    };
    VegaPlotView.prototype.render = function () {
        _super.prototype.render.call(this);
        this._plot();
    };
    VegaPlotView.prototype._plot = function () {
        if (!this.model.data || !window.vegaEmbed)
            return;
        if (this.model.data_sources && (Object.keys(this.model.data_sources).length > 0)) {
            var datasets = this._fetch_datasets();
            if ('data' in datasets) {
                this.model.data.data['values'] = datasets['data'];
                delete datasets['data'];
            }
            if (this.model.data.data !== undefined) {
                for (var _i = 0, _a = this.model.data.data; _i < _a.length; _i++) {
                    var d = _a[_i];
                    if (d.name in datasets) {
                        d['values'] = datasets[d.name];
                        delete datasets[d.name];
                    }
                }
            }
            this.model.data['datasets'] = datasets;
        }
        window.vegaEmbed(this.el, this.model.data, { actions: false });
    };
    VegaPlotView.__name__ = "VegaPlotView";
    return VegaPlotView;
}(html_box_1.HTMLBoxView));
exports.VegaPlotView = VegaPlotView;
var VegaPlot = /** @class */ (function (_super) {
    __extends(VegaPlot, _super);
    function VegaPlot(attrs) {
        return _super.call(this, attrs) || this;
    }
    VegaPlot.init_VegaPlot = function () {
        this.prototype.default_view = VegaPlotView;
        this.define({
            data: [p.Any],
            data_sources: [p.Any],
        });
    };
    VegaPlot.__name__ = "VegaPlot";
    VegaPlot.__module__ = "panel.models.vega";
    return VegaPlot;
}(html_box_1.HTMLBox));
exports.VegaPlot = VegaPlot;
VegaPlot.init_VegaPlot();
//# sourceMappingURL=vega.js.map