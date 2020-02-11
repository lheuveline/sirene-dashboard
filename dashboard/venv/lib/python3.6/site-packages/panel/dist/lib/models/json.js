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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var p = __importStar(require("@bokehjs/core/properties"));
var markup_1 = require("@bokehjs/models/widgets/markup");
var json_formatter_js_1 = __importDefault(require("json-formatter-js"));
var layout_1 = require("./layout");
var JSONView = /** @class */ (function (_super) {
    __extends(JSONView, _super);
    function JSONView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    JSONView.prototype.render = function () {
        _super.prototype.render.call(this);
        var text = this.model.text.replace(/(\r\n|\n|\r)/gm, "").replace("'", '"');
        var json;
        try {
            json = window.JSON.parse(text);
        }
        catch (err) {
            this.markup_el.innerHTML = "<b>Invalid JSON:</b> " + err.toString();
            return;
        }
        var config = { hoverPreviewEnabled: this.model.hover_preview, theme: this.model.theme };
        var formatter = new json_formatter_js_1.default(json, this.model.depth, config);
        var rendered = formatter.render();
        var style = "border-radius: 5px; padding: 10px;";
        if (this.model.theme == "dark")
            rendered.style.cssText = "background-color: rgb(30, 30, 30);" + style;
        else
            rendered.style.cssText = style;
        this.markup_el.append(rendered);
    };
    JSONView.__name__ = "JSONView";
    return JSONView;
}(layout_1.PanelMarkupView));
exports.JSONView = JSONView;
var Theme = ["dark", "light"];
var JSON = /** @class */ (function (_super) {
    __extends(JSON, _super);
    function JSON(attrs) {
        return _super.call(this, attrs) || this;
    }
    JSON.init_JSON = function () {
        this.prototype.default_view = JSONView;
        this.define({
            depth: [p.Number, 1],
            hover_preview: [p.Boolean, false],
            theme: [p.Enum(Theme), "dark"],
        });
    };
    JSON.__name__ = "JSON";
    JSON.__module__ = "panel.models.markup";
    return JSON;
}(markup_1.Markup));
exports.JSON = JSON;
JSON.init_JSON();
//# sourceMappingURL=json.js.map