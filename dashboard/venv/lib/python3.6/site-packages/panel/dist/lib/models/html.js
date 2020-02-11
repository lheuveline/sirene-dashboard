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
function htmlDecode(input) {
    var doc = new DOMParser().parseFromString(input, "text/html");
    return doc.documentElement.textContent;
}
var HTMLView = /** @class */ (function (_super) {
    __extends(HTMLView, _super);
    function HTMLView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    HTMLView.prototype.render = function () {
        _super.prototype.render.call(this);
        var decoded = htmlDecode(this.model.text);
        var html = decoded || this.model.text;
        if (!html) {
            this.markup_el.innerHTML = '';
            return;
        }
        this.markup_el.innerHTML = html;
        Array.from(this.markup_el.querySelectorAll("script")).forEach(function (oldScript) {
            var newScript = document.createElement("script");
            Array.from(oldScript.attributes)
                .forEach(function (attr) { return newScript.setAttribute(attr.name, attr.value); });
            newScript.appendChild(document.createTextNode(oldScript.innerHTML));
            if (oldScript.parentNode)
                oldScript.parentNode.replaceChild(newScript, oldScript);
        });
    };
    HTMLView.__name__ = "HTMLView";
    return HTMLView;
}(layout_1.PanelMarkupView));
exports.HTMLView = HTMLView;
var HTML = /** @class */ (function (_super) {
    __extends(HTML, _super);
    function HTML(attrs) {
        return _super.call(this, attrs) || this;
    }
    HTML.init_HTML = function () {
        this.prototype.default_view = HTMLView;
    };
    HTML.__name__ = "HTML";
    HTML.__module__ = "panel.models.markup";
    return HTML;
}(markup_1.Markup));
exports.HTML = HTML;
HTML.init_HTML();
//# sourceMappingURL=html.js.map