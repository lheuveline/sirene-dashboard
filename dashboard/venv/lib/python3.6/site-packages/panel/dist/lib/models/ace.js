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
var dom_1 = require("@bokehjs/core/dom");
var layout_1 = require("./layout");
function ID() {
    // Math.random should be unique because of its seeding algorithm.
    // Convert it to base 36 (numbers + letters), and grab the first 9 characters
    // after the decimal.
    return '_' + Math.random().toString(36).substr(2, 9);
}
var AcePlotView = /** @class */ (function (_super) {
    __extends(AcePlotView, _super);
    function AcePlotView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    AcePlotView.prototype.initialize = function () {
        _super.prototype.initialize.call(this);
        this._ace = window.ace;
        this._container = dom_1.div({
            id: ID(),
            style: {
                width: "100%",
                height: "100%"
            }
        });
    };
    AcePlotView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.code.change, function () { return _this._update_code_from_model(); });
        this.connect(this.model.properties.theme.change, function () { return _this._update_theme(); });
        this.connect(this.model.properties.language.change, function () { return _this._update_language(); });
        this.connect(this.model.properties.annotations.change, function () { return _this._add_annotations(); });
        this.connect(this.model.properties.readonly.change, function () {
            _this._editor.setReadOnly(_this.model.readonly);
        });
    };
    AcePlotView.prototype.render = function () {
        var _this = this;
        _super.prototype.render.call(this);
        if (!(this._container === this.el.childNodes[0]))
            this.el.appendChild(this._container);
        this._container.textContent = this.model.code;
        this._editor = this._ace.edit(this._container.id);
        this._editor.setTheme("ace/theme/" + ("" + this.model.theme));
        this._editor.session.setMode("ace/mode/" + ("" + this.model.language));
        this._editor.setReadOnly(this.model.readonly);
        this._langTools = this._ace.require('ace/ext/language_tools');
        this._editor.setOptions({
            enableBasicAutocompletion: true,
            enableSnippets: true,
            fontFamily: "monospace",
        });
        this._editor.on('change', function () { return _this._update_code_from_editor(); });
    };
    AcePlotView.prototype._update_code_from_model = function () {
        if (this._editor && this._editor.getValue() != this.model.code)
            this._editor.setValue(this.model.code);
    };
    AcePlotView.prototype._update_code_from_editor = function () {
        if (this._editor.getValue() != this.model.code) {
            this.model.code = this._editor.getValue();
        }
    };
    AcePlotView.prototype._update_theme = function () {
        this._editor.setTheme("ace/theme/" + ("" + this.model.theme));
    };
    AcePlotView.prototype._update_language = function () {
        this._editor.session.setMode("ace/mode/" + ("" + this.model.language));
    };
    AcePlotView.prototype._add_annotations = function () {
        this._editor.session.setAnnotations(this.model.annotations);
    };
    AcePlotView.prototype.after_layout = function () {
        _super.prototype.after_layout.call(this);
        this._editor.resize();
    };
    AcePlotView.__name__ = "AcePlotView";
    return AcePlotView;
}(layout_1.PanelHTMLBoxView));
exports.AcePlotView = AcePlotView;
var AcePlot = /** @class */ (function (_super) {
    __extends(AcePlot, _super);
    function AcePlot(attrs) {
        return _super.call(this, attrs) || this;
    }
    AcePlot.init_AcePlot = function () {
        this.prototype.default_view = AcePlotView;
        this.define({
            code: [p.String],
            language: [p.String, 'python'],
            theme: [p.String, 'chrome'],
            annotations: [p.Array, []],
            readonly: [p.Boolean, false]
        });
        this.override({
            height: 300,
            width: 300
        });
    };
    AcePlot.__name__ = "AcePlot";
    AcePlot.__module__ = "panel.models.ace";
    return AcePlot;
}(html_box_1.HTMLBox));
exports.AcePlot = AcePlot;
AcePlot.init_AcePlot();
//# sourceMappingURL=ace.js.map