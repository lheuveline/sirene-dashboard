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
var KaTeXView = /** @class */ (function (_super) {
    __extends(KaTeXView, _super);
    function KaTeXView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    KaTeXView.prototype.render = function () {
        _super.prototype.render.call(this);
        this.markup_el.innerHTML = this.model.text;
        if (!window.renderMathInElement) {
            return;
        }
        window.renderMathInElement(this.el, {
            delimiters: [
                { left: "$$", right: "$$", display: true },
                { left: "\\[", right: "\\]", display: true },
                { left: "$", right: "$", display: false },
                { left: "\\(", right: "\\)", display: false }
            ]
        });
    };
    KaTeXView.__name__ = "KaTeXView";
    return KaTeXView;
}(layout_1.PanelMarkupView));
exports.KaTeXView = KaTeXView;
var KaTeX = /** @class */ (function (_super) {
    __extends(KaTeX, _super);
    function KaTeX(attrs) {
        return _super.call(this, attrs) || this;
    }
    KaTeX.init_KaTeX = function () {
        this.prototype.default_view = KaTeXView;
    };
    KaTeX.__name__ = "KaTeX";
    KaTeX.__module__ = "panel.models.katex";
    return KaTeX;
}(markup_1.Markup));
exports.KaTeX = KaTeX;
KaTeX.init_KaTeX();
//# sourceMappingURL=katex.js.map