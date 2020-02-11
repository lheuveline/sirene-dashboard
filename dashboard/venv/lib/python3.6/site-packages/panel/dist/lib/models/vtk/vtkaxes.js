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
var gl_matrix_1 = require("gl-matrix");
var model_1 = require("@bokehjs/model");
var p = __importStar(require("@bokehjs/core/properties"));
var vtk_utils_1 = require("./vtk_utils");
var VTKAxes = /** @class */ (function (_super) {
    __extends(VTKAxes, _super);
    function VTKAxes(attrs) {
        return _super.call(this, attrs) || this;
    }
    VTKAxes.init_VTKAxes = function () {
        this.define({
            origin: [p.Array],
            xticker: [p.Instance],
            yticker: [p.Instance],
            zticker: [p.Instance],
            digits: [p.Number, 1],
            show_grid: [p.Boolean, true],
            grid_opacity: [p.Number, 0.1],
            axes_opacity: [p.Number, 1],
            fontsize: [p.Number, 12],
        });
    };
    Object.defineProperty(VTKAxes.prototype, "xticks", {
        get: function () {
            return this.xticker.ticks;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKAxes.prototype, "yticks", {
        get: function () {
            return this.yticker.ticks;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKAxes.prototype, "zticks", {
        get: function () {
            return this.zticker.ticks;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKAxes.prototype, "xlabels", {
        get: function () {
            var _this = this;
            return this.xticker.labels ? this.xticker.labels : this.xticks.map(function (elem) { return elem.toFixed(_this.digits); });
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKAxes.prototype, "ylabels", {
        get: function () {
            var _this = this;
            return this.yticker.labels ? this.yticker.labels : this.yticks.map(function (elem) { return elem.toFixed(_this.digits); });
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKAxes.prototype, "zlabels", {
        get: function () {
            var _this = this;
            return this.zticker.labels ? this.zticker.labels : this.zticks.map(function (elem) { return elem.toFixed(_this.digits); });
        },
        enumerable: true,
        configurable: true
    });
    VTKAxes.prototype._make_grid_lines = function (n, m, offset) {
        var out = [];
        for (var i = 0; i < n - 1; i++) {
            for (var j = 0; j < m - 1; j++) {
                var v0 = i * m + j + offset;
                var v1 = i * m + j + 1 + offset;
                var v2 = (i + 1) * m + j + 1 + offset;
                var v3 = (i + 1) * m + j + offset;
                var line = [5, v0, v1, v2, v3, v0];
                out.push(line);
            }
        }
        return out;
    };
    VTKAxes.prototype._create_grid_axes = function () {
        var pts = [];
        pts.push(vtk_utils_1.cartesian_product(this.xticks, this.yticks, [this.origin[2]])); //xy
        pts.push(vtk_utils_1.cartesian_product([this.origin[0]], this.yticks, this.zticks)); //yz
        pts.push(vtk_utils_1.cartesian_product(this.xticks, [this.origin[1]], this.zticks)); //xz
        var polys = [];
        var offset = 0;
        polys.push(this._make_grid_lines(this.xticks.length, this.yticks.length, offset)); //xy
        offset += this.xticks.length * this.yticks.length;
        polys.push(this._make_grid_lines(this.yticks.length, this.zticks.length, offset)); //yz
        offset += this.yticks.length * this.zticks.length;
        polys.push(this._make_grid_lines(this.xticks.length, this.zticks.length, offset)); //xz
        var gridPolyData = vtk_utils_1.vtk({
            vtkClass: 'vtkPolyData',
            points: {
                vtkClass: 'vtkPoints',
                dataType: 'Float32Array',
                numberOfComponents: 3,
                values: pts.flat(2),
            },
            lines: {
                vtkClass: 'vtkCellArray',
                dataType: 'Uint32Array',
                values: polys.flat(2)
            }
        });
        var gridMapper = vtk_utils_1.vtkns.Mapper.newInstance();
        var gridActor = vtk_utils_1.vtkns.Actor.newInstance();
        gridMapper.setInputData(gridPolyData);
        gridActor.setMapper(gridMapper);
        gridActor.getProperty().setOpacity(this.grid_opacity);
        gridActor.setVisibility(this.show_grid);
        return gridActor;
    };
    VTKAxes.prototype.create_axes = function (canvas) {
        var _this = this;
        var points = [this.xticks, this.yticks, this.zticks].map(function (arr, axis) {
            var coords = null;
            switch (axis) {
                case 0:
                    coords = vtk_utils_1.cartesian_product(arr, [_this.origin[1]], [_this.origin[2]]);
                    break;
                case 1:
                    coords = vtk_utils_1.cartesian_product([_this.origin[0]], arr, [_this.origin[2]]);
                    break;
                case 2:
                    coords = vtk_utils_1.cartesian_product([_this.origin[0]], [_this.origin[1]], arr);
                    break;
            }
            return coords;
        }).flat(2);
        var axesPolyData = vtk_utils_1.vtk({
            vtkClass: 'vtkPolyData',
            points: {
                vtkClass: 'vtkPoints',
                dataType: 'Float32Array',
                numberOfComponents: 3,
                values: points,
            },
            lines: {
                vtkClass: 'vtkCellArray',
                dataType: 'Uint32Array',
                values: [2, 0, this.xticks.length - 1,
                    2, this.xticks.length, this.xticks.length + this.yticks.length - 1,
                    2, this.xticks.length + this.yticks.length, this.xticks.length + this.yticks.length + this.zticks.length - 1]
            }
        });
        var psMapper = vtk_utils_1.vtkns.PixelSpaceCallbackMapper.newInstance();
        psMapper.setInputData(axesPolyData);
        psMapper.setUseZValues(true);
        psMapper.setCallback(function (coordsList, camera, aspect) {
            var textCtx = canvas.getContext("2d");
            if (textCtx) {
                var dims_1 = {
                    height: canvas.clientHeight * window.devicePixelRatio,
                    width: canvas.clientWidth * window.devicePixelRatio
                };
                var dataPoints_1 = psMapper.getInputData().getPoints();
                var viewMatrix_1 = camera.getViewMatrix();
                gl_matrix_1.mat4.transpose(viewMatrix_1, viewMatrix_1);
                var projMatrix_1 = camera.getProjectionMatrix(aspect, -1, 1);
                gl_matrix_1.mat4.transpose(projMatrix_1, projMatrix_1);
                textCtx.clearRect(0, 0, dims_1.width, dims_1.height);
                coordsList.forEach(function (xy, idx) {
                    var pdPoint = dataPoints_1.getPoint(idx);
                    var vc = gl_matrix_1.vec3.fromValues(pdPoint[0], pdPoint[1], pdPoint[2]);
                    gl_matrix_1.vec3.transformMat4(vc, vc, viewMatrix_1);
                    vc[2] += 0.05; // sensibility
                    gl_matrix_1.vec3.transformMat4(vc, vc, projMatrix_1);
                    if (vc[2] - 0.001 < xy[3]) {
                        textCtx.font = '30px serif';
                        textCtx.textAlign = 'center';
                        textCtx.textBaseline = 'alphabetic';
                        textCtx.fillText(".", xy[0], dims_1.height - xy[1] + 2);
                        textCtx.font = _this.fontsize * window.devicePixelRatio + "px serif";
                        textCtx.textAlign = 'right';
                        textCtx.textBaseline = 'top';
                        var label = void 0;
                        if (idx < _this.xticks.length)
                            label = _this.xlabels[idx];
                        else if (idx >= _this.xticks.length && idx < _this.xticks.length + _this.yticks.length)
                            label = _this.ylabels[idx - _this.xticks.length];
                        else
                            label = _this.zlabels[idx - (_this.xticks.length + _this.yticks.length)];
                        textCtx.fillText("" + label, xy[0], dims_1.height - xy[1]);
                    }
                });
            }
        });
        var psActor = vtk_utils_1.vtkns.Actor.newInstance();
        psActor.setMapper(psMapper);
        var axesMapper = vtk_utils_1.vtkns.Mapper.newInstance();
        axesMapper.setInputData(axesPolyData);
        var axesActor = vtk_utils_1.vtkns.Actor.newInstance();
        axesActor.setMapper(axesMapper);
        axesActor.getProperty().setOpacity(this.axes_opacity);
        var gridActor = this._create_grid_axes();
        return { psActor: psActor, axesActor: axesActor, gridActor: gridActor };
    };
    VTKAxes.__name__ = "VTKAxes";
    VTKAxes.__module__ = "panel.models.vtk";
    return VTKAxes;
}(model_1.Model));
exports.VTKAxes = VTKAxes;
VTKAxes.init_VTKAxes();
//# sourceMappingURL=vtkaxes.js.map