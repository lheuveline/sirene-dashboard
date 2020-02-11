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
var markup_1 = require("@bokehjs/models/widgets/markup");
var layout_1 = require("./layout");
var MathJaxView = /** @class */ (function (_super) {
    __extends(MathJaxView, _super);
    function MathJaxView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    MathJaxView.prototype.initialize = function () {
        _super.prototype.initialize.call(this);
        this._hub = window.MathJax.Hub;
        this._hub.Config({
            tex2jax: { inlineMath: [['$', '$'], ['\\(', '\\)']] }
        });
    };
    MathJaxView.prototype.render = function () {
        _super.prototype.render.call(this);
        if (!this._hub) {
            return;
        }
        this.markup_el.innerHTML = this.model.text;
        this._hub.Queue(["Typeset", this._hub, this.markup_el]);
    };
    MathJaxView.__name__ = "MathJaxView";
    return MathJaxView;
}(layout_1.PanelMarkupView));
exports.MathJaxView = MathJaxView;
var MathJax = /** @class */ (function (_super) {
    __extends(MathJax, _super);
    function MathJax(attrs) {
        return _super.call(this, attrs) || this;
    }
    MathJax.init_MathJax = function () {
        this.prototype.default_view = MathJaxView;
    };
    MathJax.__name__ = "MathJax";
    MathJax.__module__ = "panel.models.mathjax";
    return MathJax;
}(markup_1.Markup));
exports.MathJax = MathJax;
MathJax.init_MathJax();
//# sourceMappingURL=mathjax.js.map