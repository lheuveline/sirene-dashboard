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
var layout_1 = require("../layout");
var dom_1 = require("@bokehjs/core/dom");
var vtk_utils_1 = require("./vtk_utils");
var VTKHTMLBoxView = /** @class */ (function (_super) {
    __extends(VTKHTMLBoxView, _super);
    function VTKHTMLBoxView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    VTKHTMLBoxView.prototype.render = function () {
        _super.prototype.render.call(this);
        this._vtk_container = dom_1.div();
        layout_1.set_size(this._vtk_container, this.model);
        this.el.appendChild(this._vtk_container);
        this._vtk_renwin = vtk_utils_1.vtkns.FullScreenRenderWindow.newInstance({
            rootContainer: this.el,
            container: this._vtk_container
        });
        this._remove_default_key_binding();
    };
    VTKHTMLBoxView.prototype.after_layout = function () {
        _super.prototype.after_layout.call(this);
        this._vtk_renwin.resize();
    };
    VTKHTMLBoxView.prototype._remove_default_key_binding = function () {
        var interactor = this._vtk_renwin.getInteractor();
        document.querySelector('body').removeEventListener('keypress', interactor.handleKeyPress);
        document.querySelector('body').removeEventListener('keydown', interactor.handleKeyDown);
        document.querySelector('body').removeEventListener('keyup', interactor.handleKeyUp);
    };
    VTKHTMLBoxView.__name__ = "VTKHTMLBoxView";
    return VTKHTMLBoxView;
}(layout_1.PanelHTMLBoxView));
exports.VTKHTMLBoxView = VTKHTMLBoxView;
//# sourceMappingURL=vtk_layout.js.map