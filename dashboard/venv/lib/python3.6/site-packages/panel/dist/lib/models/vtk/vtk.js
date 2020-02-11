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
var dom_1 = require("@bokehjs/core/dom");
var p = __importStar(require("@bokehjs/core/properties"));
var object_1 = require("@bokehjs/core/util/object");
var html_box_1 = require("@bokehjs/models/layouts/html_box");
var vtk_layout_1 = require("./vtk_layout");
var vtk_utils_1 = require("./vtk_utils");
var VTKPlotView = /** @class */ (function (_super) {
    __extends(VTKPlotView, _super);
    function VTKPlotView() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this._setting = false;
        _this._axes_initialized = false;
        return _this;
    }
    VTKPlotView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.data.change, function () {
            _this.invalidate_render();
        });
        this.connect(this.model.properties.camera.change, function () { return _this._set_camera_state(); });
        this.connect(this.model.properties.orientation_widget.change, function () {
            _this._orientation_widget_visbility(_this.model.orientation_widget);
        });
        this.connect(this.model.properties.axes.change, function () {
            _this._delete_axes();
            if (_this.model.axes)
                _this._set_axes();
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.el.addEventListener('mouseenter', function () {
            var interactor = _this._vtk_renwin.getInteractor();
            if (_this.model.enable_keybindings) {
                document.querySelector('body').addEventListener('keypress', interactor.handleKeyPress);
                document.querySelector('body').addEventListener('keydown', interactor.handleKeyDown);
                document.querySelector('body').addEventListener('keyup', interactor.handleKeyUp);
            }
        });
        this.el.addEventListener('mouseleave', function () {
            var interactor = _this._vtk_renwin.getInteractor();
            document.querySelector('body').removeEventListener('keypress', interactor.handleKeyPress);
            document.querySelector('body').removeEventListener('keydown', interactor.handleKeyDown);
            document.querySelector('body').removeEventListener('keyup', interactor.handleKeyUp);
        });
    };
    VTKPlotView.prototype.render = function () {
        var _this = this;
        _super.prototype.render.call(this);
        this.model.renderer_el = this._vtk_renwin;
        this._orientationWidget = null;
        this._axes = null;
        this._axes_initialized = false;
        this._plot();
        this._vtk_renwin.getRenderer().getActiveCamera().onModified(function () { return _this._get_camera_state(); });
    };
    VTKPlotView.prototype.after_layout = function () {
        if (!this._axes_initialized) {
            this._render_axes_canvas();
            this._axes_initialized = true;
        }
    };
    VTKPlotView.prototype._create_orientation_widget = function () {
        var _this = this;
        var axes = vtk_utils_1.vtkns.AxesActor.newInstance();
        // add orientation widget
        var orientationWidget = vtk_utils_1.vtkns.OrientationMarkerWidget.newInstance({
            actor: axes,
            interactor: this._vtk_renwin.getInteractor(),
        });
        orientationWidget.setEnabled(true);
        orientationWidget.setViewportCorner(vtk_utils_1.vtkns.OrientationMarkerWidget.Corners.BOTTOM_RIGHT);
        orientationWidget.setViewportSize(0.15);
        orientationWidget.setMinPixelSize(100);
        orientationWidget.setMaxPixelSize(300);
        this._orientationWidget = orientationWidget;
        var widgetManager = vtk_utils_1.vtkns.WidgetManager.newInstance();
        widgetManager.setRenderer(orientationWidget.getRenderer());
        var widget = vtk_utils_1.vtkns.InteractiveOrientationWidget.newInstance();
        widget.placeWidget(axes.getBounds());
        widget.setBounds(axes.getBounds());
        widget.setPlaceFactor(1);
        var vw = widgetManager.addWidget(widget);
        this._widgetManager = widgetManager;
        // Manage user interaction
        vw.onOrientationChange(function (_a) {
            var direction = _a.direction;
            var camera = _this._vtk_renwin.getRenderer().getActiveCamera();
            var focalPoint = camera.getFocalPoint();
            var position = camera.getPosition();
            var viewUp = camera.getViewUp();
            var distance = Math.sqrt(Math.pow(position[0] - focalPoint[0], 2) +
                Math.pow(position[1] - focalPoint[1], 2) +
                Math.pow(position[2] - focalPoint[2], 2));
            camera.setPosition(focalPoint[0] + direction[0] * distance, focalPoint[1] + direction[1] * distance, focalPoint[2] + direction[2] * distance);
            if (direction[0])
                camera.setViewUp(vtk_utils_1.majorAxis(viewUp, 1, 2));
            if (direction[1])
                camera.setViewUp(vtk_utils_1.majorAxis(viewUp, 0, 2));
            if (direction[2])
                camera.setViewUp(vtk_utils_1.majorAxis(viewUp, 0, 1));
            _this._orientationWidget.updateMarkerOrientation();
            _this._vtk_renwin.getRenderer().resetCameraClippingRange();
            _this._vtk_renwin.getRenderWindow().render();
        });
        this._orientation_widget_visbility(this.model.orientation_widget);
    };
    VTKPlotView.prototype._render_axes_canvas = function () {
        var _this = this;
        var canvas_list = this._vtk_container.getElementsByTagName('canvas');
        if (canvas_list.length != 1)
            throw Error('Error at initialization of the 3D scene, container should have one and only one canvas');
        else
            canvas_list[0].classList.add('scene3d-canvas');
        var axes_canvas = dom_1.canvas({
            style: {
                position: "absolute",
                top: "0",
                left: "0",
                width: "100%",
                height: "100%"
            }
        });
        axes_canvas.classList.add('axes-canvas');
        this._vtk_container.appendChild(axes_canvas);
        this._vtk_renwin.setResizeCallback(function () {
            var dims = _this._vtk_container.getBoundingClientRect();
            var width = Math.floor(dims.width * window.devicePixelRatio);
            var height = Math.floor(dims.height * window.devicePixelRatio);
            axes_canvas.setAttribute('width', width.toFixed());
            axes_canvas.setAttribute('height', height.toFixed());
        });
    };
    VTKPlotView.prototype._orientation_widget_visbility = function (visbility) {
        this._orientationWidget.setEnabled(visbility);
        if (visbility)
            this._widgetManager.enablePicking();
        else
            this._widgetManager.disablePicking();
        this._orientationWidget.updateMarkerOrientation();
        this._vtk_renwin.getRenderWindow().render();
    };
    VTKPlotView.prototype._get_camera_state = function () {
        if (!this._setting) {
            this._setting = true;
            var state = object_1.clone(this._vtk_renwin.getRenderer().getActiveCamera().get());
            delete state.classHierarchy;
            delete state.vtkObject;
            delete state.vtkCamera;
            delete state.viewPlaneNormal;
            this.model.camera = state;
            this._setting = false;
        }
    };
    VTKPlotView.prototype._set_camera_state = function () {
        if (!this._setting) {
            this._setting = true;
            try {
                this._vtk_renwin.getRenderer().getActiveCamera().set(this.model.camera);
            }
            finally {
                this._setting = false;
            }
            if (this._orientationWidget != null) {
                this._orientationWidget.updateMarkerOrientation();
            }
            this._vtk_renwin.getRenderer().resetCameraClippingRange();
            this._vtk_renwin.getRenderWindow().render();
        }
    };
    VTKPlotView.prototype._delete_axes = function () {
        var _this = this;
        if (this._axes == null)
            return;
        Object.keys(this._axes).forEach(function (key) { return _this._vtk_renwin.getRenderer().removeActor(_this._axes[key]); });
        var axesCanvas = this._vtk_renwin.getContainer().getElementsByClassName('axes-canvas')[0];
        var textCtx = axesCanvas.getContext("2d");
        if (textCtx)
            textCtx.clearRect(0, 0, axesCanvas.clientWidth * window.devicePixelRatio, axesCanvas.clientHeight * window.devicePixelRatio);
        this._axes = null;
    };
    VTKPlotView.prototype._set_axes = function () {
        if (this.model.axes) {
            var axesCanvas = this._vtk_renwin.getContainer().getElementsByClassName('axes-canvas')[0];
            var _a = this.model.axes.create_axes(axesCanvas), psActor = _a.psActor, axesActor = _a.axesActor, gridActor = _a.gridActor;
            this._axes = { psActor: psActor, axesActor: axesActor, gridActor: gridActor };
            this._vtk_renwin.getRenderer().addActor(psActor);
            this._vtk_renwin.getRenderer().addActor(axesActor);
            this._vtk_renwin.getRenderer().addActor(gridActor);
        }
    };
    VTKPlotView.prototype._plot = function () {
        var _this = this;
        if (!this.model.data) {
            this._vtk_renwin.getRenderWindow().render();
            return;
        }
        var dataAccessHelper = vtk_utils_1.vtkns.DataAccessHelper.get('zip', {
            zipContent: atob(this.model.data),
            callback: function (_zip) {
                var sceneImporter = vtk_utils_1.vtkns.HttpSceneLoader.newInstance({
                    renderer: _this._vtk_renwin.getRenderer(),
                    dataAccessHelper: dataAccessHelper,
                });
                var fn = vtk_utils_1.vtk.macro.debounce(function () {
                    if (_this._orientationWidget == null)
                        _this._create_orientation_widget();
                    if (_this._axes == null && _this.model.axes)
                        _this._set_axes();
                    _this._set_camera_state();
                }, 100);
                sceneImporter.setUrl('index.json');
                sceneImporter.onReady(fn);
            }
        });
    };
    VTKPlotView.__name__ = "VTKPlotView";
    return VTKPlotView;
}(vtk_layout_1.VTKHTMLBoxView));
exports.VTKPlotView = VTKPlotView;
var VTKPlot = /** @class */ (function (_super) {
    __extends(VTKPlot, _super);
    function VTKPlot(attrs) {
        var _this = _super.call(this, attrs) || this;
        _this.renderer_el = null;
        _this.outline = vtk_utils_1.vtkns.OutlineFilter.newInstance(); //use to display bouding box of a selected actor
        var mapper = vtk_utils_1.vtkns.Mapper.newInstance();
        mapper.setInputConnection(_this.outline.getOutputPort());
        _this.outline_actor = vtk_utils_1.vtkns.Actor.newInstance();
        _this.outline_actor.setMapper(mapper);
        return _this;
    }
    VTKPlot.prototype.getActors = function () {
        return this.renderer_el.getRenderer().getActors();
    };
    VTKPlot.init_VTKPlot = function () {
        this.prototype.default_view = VTKPlotView;
        this.define({
            axes: [p.Instance],
            camera: [p.Any],
            data: [p.String],
            enable_keybindings: [p.Boolean, false],
            orientation_widget: [p.Boolean, false],
        });
        this.override({
            height: 300,
            width: 300
        });
    };
    VTKPlot.__name__ = "VTKPlot";
    VTKPlot.__module__ = "panel.models.vtk";
    return VTKPlot;
}(html_box_1.HTMLBox));
exports.VTKPlot = VTKPlot;
VTKPlot.init_VTKPlot();
//# sourceMappingURL=vtk.js.map