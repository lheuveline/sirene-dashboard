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
Object.defineProperty(exports, "__esModule", { value: true });
var layoutable_1 = require("@bokehjs/core/layout/layoutable");
var types_1 = require("@bokehjs/core/layout/types");
var dom_1 = require("@bokehjs/core/dom");
var markup_1 = require("@bokehjs/models/widgets/markup");
var html_box_1 = require("@bokehjs/models/layouts/html_box");
function set_size(el, model) {
    var width_policy = model.width != null ? "fixed" : "fit";
    var height_policy = model.height != null ? "fixed" : "fit";
    var sizing_mode = model.sizing_mode;
    if (sizing_mode != null) {
        if (sizing_mode == "fixed")
            width_policy = height_policy = "fixed";
        else if (sizing_mode == "stretch_both")
            width_policy = height_policy = "max";
        else if (sizing_mode == "stretch_width")
            width_policy = "max";
        else if (sizing_mode == "stretch_height")
            height_policy = "max";
        else {
            switch (sizing_mode) {
                case "scale_width":
                    width_policy = "max";
                    height_policy = "min";
                    break;
                case "scale_height":
                    width_policy = "min";
                    height_policy = "max";
                    break;
                case "scale_both":
                    width_policy = "max";
                    height_policy = "max";
                    break;
                default:
                    throw new Error("unreachable");
            }
        }
    }
    if (width_policy == "fixed" && model.width)
        el.style.width = model.width + "px";
    else if (width_policy == "max")
        el.style.width = "100%";
    if (height_policy == "fixed" && model.height)
        el.style.height = model.height + "px";
    else if (height_policy == "max")
        el.style.height = "100%";
}
exports.set_size = set_size;
var CachedVariadicBox = /** @class */ (function (_super) {
    __extends(CachedVariadicBox, _super);
    function CachedVariadicBox(el, sizing_mode, changed) {
        var _this = _super.call(this) || this;
        _this.el = el;
        _this.sizing_mode = sizing_mode;
        _this.changed = changed;
        _this._cache = {};
        _this._cache_count = {};
        return _this;
    }
    CachedVariadicBox.prototype._measure = function (viewport) {
        var _this = this;
        var key = [viewport.width, viewport.height, this.sizing_mode];
        var key_str = key.toString();
        // If sizing mode is responsive and has changed since last render
        // we have to wait until second rerender to use cached value
        var min_count = (!this.changed || (this.sizing_mode == 'fixed') || (this.sizing_mode == null)) ? 0 : 1;
        if ((key_str in this._cache) && (this._cache_count[key_str] >= min_count)) {
            this._cache_count[key_str] = this._cache_count[key_str] + 1;
            return this._cache[key_str];
        }
        var bounded = new types_1.Sizeable(viewport).bounded_to(this.sizing.size);
        var size = dom_1.sized(this.el, bounded, function () {
            var content = new types_1.Sizeable(dom_1.content_size(_this.el));
            var _a = dom_1.extents(_this.el), border = _a.border, padding = _a.padding;
            return content.grow_by(border).grow_by(padding).map(Math.ceil);
        });
        this._cache[key_str] = size;
        this._cache_count[key_str] = 0;
        return size;
    };
    CachedVariadicBox.__name__ = "CachedVariadicBox";
    return CachedVariadicBox;
}(layoutable_1.Layoutable));
exports.CachedVariadicBox = CachedVariadicBox;
var PanelMarkupView = /** @class */ (function (_super) {
    __extends(PanelMarkupView, _super);
    function PanelMarkupView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    PanelMarkupView.prototype._update_layout = function () {
        var changed = ((this._prev_sizing_mode !== undefined) &&
            (this._prev_sizing_mode !== this.model.sizing_mode));
        this._prev_sizing_mode = this.model.sizing_mode;
        this.layout = new CachedVariadicBox(this.el, this.model.sizing_mode, changed);
        this.layout.set_sizing(this.box_sizing());
    };
    PanelMarkupView.prototype.render = function () {
        _super.prototype.render.call(this);
        set_size(this.markup_el, this.model);
    };
    PanelMarkupView.__name__ = "PanelMarkupView";
    return PanelMarkupView;
}(markup_1.MarkupView));
exports.PanelMarkupView = PanelMarkupView;
var PanelHTMLBoxView = /** @class */ (function (_super) {
    __extends(PanelHTMLBoxView, _super);
    function PanelHTMLBoxView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    PanelHTMLBoxView.prototype._update_layout = function () {
        var changed = ((this._prev_sizing_mode !== undefined) &&
            (this._prev_sizing_mode !== this.model.sizing_mode));
        this._prev_sizing_mode = this.model.sizing_mode;
        this.layout = new CachedVariadicBox(this.el, this.model.sizing_mode, changed);
        this.layout.set_sizing(this.box_sizing());
    };
    PanelHTMLBoxView.prototype.render = function () {
        _super.prototype.render.call(this);
        set_size(this.el, this.model);
    };
    PanelHTMLBoxView.__name__ = "PanelHTMLBoxView";
    return PanelHTMLBoxView;
}(html_box_1.HTMLBoxView));
exports.PanelHTMLBoxView = PanelHTMLBoxView;
//# sourceMappingURL=layout.js.map