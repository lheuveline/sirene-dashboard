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
var vtk_layout_1 = require("./vtk_layout");
var vtk_utils_1 = require("./vtk_utils");
var VTKVolumePlotView = /** @class */ (function (_super) {
    __extends(VTKVolumePlotView, _super);
    function VTKVolumePlotView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    VTKVolumePlotView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.data.change, function () {
            _this.invalidate_render();
        });
        this.connect(this.model.properties.colormap.change, function () {
            _this.colormap_slector.value = _this.model.colormap;
            var event = new Event('change');
            _this.colormap_slector.dispatchEvent(event);
        });
        this.connect(this.model.properties.shadow.change, function () {
            _this.shadow_selector.value = _this.model.shadow ? '1' : '0';
            var event = new Event('change');
            _this.shadow_selector.dispatchEvent(event);
        });
        this.connect(this.model.properties.sampling.change, function () {
            _this.sampling_slider.value = _this.model.sampling.toFixed(2);
            var event = new Event('input');
            _this.sampling_slider.dispatchEvent(event);
        });
        this.connect(this.model.properties.edge_gradient.change, function () {
            _this.edge_gradient_slider.value = _this.model.edge_gradient.toFixed(2);
            var event = new Event('input');
            _this.edge_gradient_slider.dispatchEvent(event);
        });
        this.connect(this.model.properties.rescale.change, function () {
            _this._controllerWidget.setRescaleColorMap(_this.model.rescale);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.ambient.change, function () {
            _this.volume.getProperty().setAmbient(_this.model.ambient);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.diffuse.change, function () {
            _this.volume.getProperty().setDiffuse(_this.model.diffuse);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.specular.change, function () {
            _this.volume.getProperty().setSpecular(_this.model.specular);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.specular_power.change, function () {
            _this.volume.getProperty().setSpecularPower(_this.model.specular_power);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.display_volume.change, function () {
            _this._set_volume_visbility(_this.model.display_volume);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.display_slices.change, function () {
            _this._set_slices_visbility(_this.model.display_slices);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.slice_i.change, function () {
            _this.image_actor_i.getMapper().setISlice(_this.model.slice_i);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.slice_j.change, function () {
            _this.image_actor_j.getMapper().setJSlice(_this.model.slice_j);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.slice_k.change, function () {
            _this.image_actor_k.getMapper().setKSlice(_this.model.slice_k);
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.render_background.change, function () {
            var _a;
            (_a = _this._vtk_renwin.getRenderer()).setBackground.apply(_a, vtk_utils_1.hexToRGB(_this.model.render_background));
            _this._vtk_renwin.getRenderWindow().render();
        });
        this.connect(this.model.properties.interpolation.change, function () {
            _this._set_interpolation(_this.model.interpolation);
            _this._vtk_renwin.getRenderWindow().render();
        });
    };
    Object.defineProperty(VTKVolumePlotView.prototype, "volume", {
        get: function () {
            return this._controllerWidget.getActor();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKVolumePlotView.prototype, "image_actor_i", {
        get: function () {
            return this._vtk_renwin.getRenderer().getActors()[0];
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKVolumePlotView.prototype, "image_actor_j", {
        get: function () {
            return this._vtk_renwin.getRenderer().getActors()[1];
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKVolumePlotView.prototype, "image_actor_k", {
        get: function () {
            return this._vtk_renwin.getRenderer().getActors()[2];
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKVolumePlotView.prototype, "shadow_selector", {
        get: function () {
            return this.el.querySelector('.js-shadow');
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKVolumePlotView.prototype, "edge_gradient_slider", {
        get: function () {
            return this.el.querySelector('.js-edge');
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKVolumePlotView.prototype, "sampling_slider", {
        get: function () {
            return this.el.querySelector('.js-spacing');
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(VTKVolumePlotView.prototype, "colormap_slector", {
        get: function () {
            return this.el.querySelector('.js-color-preset');
        },
        enumerable: true,
        configurable: true
    });
    VTKVolumePlotView.prototype._set_interpolation = function (interpolation) {
        if (interpolation == 'fast_linear') {
            this.volume.getProperty().setInterpolationTypeToFastLinear();
            this.image_actor_i.getProperty().setInterpolationTypeToLinear();
        }
        else if (interpolation == 'linear') {
            this.volume.getProperty().setInterpolationTypeToLinear();
            this.image_actor_i.getProperty().setInterpolationTypeToLinear();
        }
        else { //nearest
            this.volume.getProperty().setInterpolationTypeToNearest();
            this.image_actor_i.getProperty().setInterpolationTypeToNearest();
        }
    };
    VTKVolumePlotView.prototype.render = function () {
        var _a;
        _super.prototype.render.call(this);
        this._controllerWidget = vtk_utils_1.vtkns.VolumeController.newInstance({
            size: [400, 150],
            rescaleColorMap: this.model.rescale,
        });
        this._vtk_image_data = vtk_utils_1.data2VTKImageData(this.model.data);
        this._controllerWidget.setContainer(this.el);
        this._vtk_renwin.getRenderWindow().getInteractor();
        this._vtk_renwin.getRenderWindow().getInteractor().setDesiredUpdateRate(45);
        this._plot_volume();
        this._connect_controls();
        this._plot_slices();
        this._set_volume_visbility(this.model.display_volume);
        this._set_slices_visbility(this.model.display_slices);
        (_a = this._vtk_renwin.getRenderer()).setBackground.apply(_a, vtk_utils_1.hexToRGB(this.model.render_background));
        this._set_interpolation(this.model.interpolation);
        this._vtk_renwin.getRenderer().resetCamera();
    };
    VTKVolumePlotView.prototype._connect_controls = function () {
        var _this = this;
        // Colormap selector
        this.colormap_slector.addEventListener('change', function () {
            _this.model.colormap = _this.colormap_slector.value;
        });
        if (!this.model.colormap)
            this.model.colormap = this.colormap_slector.value;
        else
            this.model.properties.colormap.change.emit();
        // Shadow selector
        this.shadow_selector.addEventListener('change', function () {
            _this.model.shadow = !!Number(_this.shadow_selector.value);
        });
        if (this.model.shadow = !!Number(this.shadow_selector.value))
            this.model.properties.shadow.change.emit();
        // Sampling slider
        this.sampling_slider.addEventListener('input', function () {
            var js_sampling_value = Number(_this.sampling_slider.value);
            if (Math.abs(_this.model.sampling - js_sampling_value) >= 5e-3)
                _this.model.sampling = js_sampling_value;
        });
        if (Math.abs(this.model.sampling - Number(this.shadow_selector.value)) >= 5e-3)
            this.model.properties.sampling.change.emit();
        // Edge Gradient slider
        this.edge_gradient_slider.addEventListener('input', function () {
            var js_edge_gradient_value = Number(_this.edge_gradient_slider.value);
            if (Math.abs(_this.model.edge_gradient - js_edge_gradient_value) >= 5e-3)
                _this.model.edge_gradient = js_edge_gradient_value;
        });
        if (Math.abs(this.model.edge_gradient - Number(this.edge_gradient_slider.value)) >= 5e-3)
            this.model.properties.edge_gradient.change.emit();
    };
    VTKVolumePlotView.prototype._plot_volume = function () {
        //Create vtk volume and add it to the scene
        var source = this._vtk_image_data;
        var actor = vtk_utils_1.vtkns.Volume.newInstance();
        var mapper = vtk_utils_1.vtkns.VolumeMapper.newInstance();
        actor.setMapper(mapper);
        mapper.setInputData(source);
        var dataArray = source.getPointData().getScalars() || source.getPointData().getArrays()[0];
        var dataRange = dataArray.getRange();
        var lookupTable = vtk_utils_1.vtkns.ColorTransferFunction.newInstance();
        var piecewiseFunction = vtk_utils_1.vtkns.PiecewiseFunction.newInstance();
        var sampleDistance = 0.7 * Math.sqrt(source.getSpacing()
            .map(function (v) { return v * v; })
            .reduce(function (a, b) { return a + b; }, 0));
        mapper.setSampleDistance(sampleDistance);
        actor.getProperty().setRGBTransferFunction(0, lookupTable);
        actor.getProperty().setScalarOpacity(0, piecewiseFunction);
        actor.getProperty().setInterpolationTypeToFastLinear();
        // actor.getProperty().setInterpolationTypeToLinear();
        // For better looking volume rendering
        // - distance in world coordinates a scalar opacity of 1.0
        actor
            .getProperty()
            .setScalarOpacityUnitDistance(0, vtk_utils_1.vtkns.BoundingBox.getDiagonalLength(source.getBounds()) / Math.max.apply(Math, source.getDimensions()));
        // - control how we emphasize surface boundaries
        //  => max should be around the average gradient magnitude for the
        //     volume or maybe average plus one std dev of the gradient magnitude
        //     (adjusted for spacing, this is a world coordinate gradient, not a
        //     pixel gradient)
        //  => max hack: (dataRange[1] - dataRange[0]) * 0.05
        actor.getProperty().setGradientOpacityMinimumValue(0, 0);
        actor
            .getProperty()
            .setGradientOpacityMaximumValue(0, (dataRange[1] - dataRange[0]) * 0.05);
        // - Use shading based on gradient
        actor.getProperty().setShade(this.model.shadow);
        actor.getProperty().setUseGradientOpacity(0, true);
        // - generic good default
        actor.getProperty().setGradientOpacityMinimumOpacity(0, 0.0);
        actor.getProperty().setGradientOpacityMaximumOpacity(0, 1.0);
        actor.getProperty().setAmbient(this.model.ambient);
        actor.getProperty().setDiffuse(this.model.diffuse);
        actor.getProperty().setSpecular(this.model.specular);
        actor.getProperty().setSpecularPower(this.model.specular_power);
        this._vtk_renwin.getRenderer().addVolume(actor);
        this._controllerWidget.setupContent(this._vtk_renwin.getRenderWindow(), actor, true);
    };
    VTKVolumePlotView.prototype._plot_slices = function () {
        var source = this._vtk_image_data;
        var image_actor_i = vtk_utils_1.vtkns.ImageSlice.newInstance();
        var image_actor_j = vtk_utils_1.vtkns.ImageSlice.newInstance();
        var image_actor_k = vtk_utils_1.vtkns.ImageSlice.newInstance();
        var image_mapper_i = vtk_utils_1.vtkns.ImageMapper.newInstance();
        var image_mapper_j = vtk_utils_1.vtkns.ImageMapper.newInstance();
        var image_mapper_k = vtk_utils_1.vtkns.ImageMapper.newInstance();
        image_mapper_i.setInputData(source);
        image_mapper_i.setISlice(this.model.slice_i);
        image_actor_i.setMapper(image_mapper_i);
        image_mapper_j.setInputData(source);
        image_mapper_j.setJSlice(this.model.slice_j);
        image_actor_j.setMapper(image_mapper_j);
        image_mapper_k.setInputData(source);
        image_mapper_k.setKSlice(this.model.slice_k);
        image_actor_k.setMapper(image_mapper_k);
        // set_color and opacity
        var piecewiseFunction = vtk_utils_1.vtkns.PiecewiseFunction.newInstance();
        piecewiseFunction.removeAllPoints();
        piecewiseFunction.addPoint(0, 1);
        var lookupTable = this.volume.getProperty().getRGBTransferFunction(0);
        var property = image_actor_i.getProperty();
        image_actor_j.setProperty(property);
        image_actor_k.setProperty(property);
        property.setRGBTransferFunction(lookupTable);
        property.setScalarOpacity(piecewiseFunction);
        var renderer = this._vtk_renwin.getRenderer();
        renderer.addActor(image_actor_i);
        renderer.addActor(image_actor_j);
        renderer.addActor(image_actor_k);
    };
    VTKVolumePlotView.prototype._set_volume_visbility = function (visibility) {
        this.volume.setVisibility(visibility);
    };
    VTKVolumePlotView.prototype._set_slices_visbility = function (visibility) {
        this._vtk_renwin.getRenderer().getActors().map(function (actor) { return actor.setVisibility(visibility); });
    };
    VTKVolumePlotView.__name__ = "VTKVolumePlotView";
    return VTKVolumePlotView;
}(vtk_layout_1.VTKHTMLBoxView));
exports.VTKVolumePlotView = VTKVolumePlotView;
var VTKVolumePlot = /** @class */ (function (_super) {
    __extends(VTKVolumePlot, _super);
    function VTKVolumePlot(attrs) {
        return _super.call(this, attrs) || this;
    }
    VTKVolumePlot.init_VTKVolumePlot = function () {
        this.prototype.default_view = VTKVolumePlotView;
        this.define({
            data: [p.Instance],
            shadow: [p.Boolean, true],
            sampling: [p.Number, 0.4],
            edge_gradient: [p.Number, 0.2],
            colormap: [p.String],
            rescale: [p.Boolean, false],
            ambient: [p.Number, 0.2],
            diffuse: [p.Number, 0.7],
            specular: [p.Number, 0.3],
            specular_power: [p.Number, 8.0],
            slice_i: [p.Int, 0],
            slice_j: [p.Int, 0],
            slice_k: [p.Int, 0],
            display_volume: [p.Boolean, true],
            display_slices: [p.Boolean, false],
            render_background: [p.String, '#52576e'],
            interpolation: [p.Any, 'fast_linear'],
        });
        this.override({
            height: 300,
            width: 300
        });
    };
    VTKVolumePlot.__name__ = "VTKVolumePlot";
    VTKVolumePlot.__module__ = "panel.models.vtk";
    return VTKVolumePlot;
}(html_box_1.HTMLBox));
exports.VTKVolumePlot = VTKVolumePlot;
VTKVolumePlot.init_VTKVolumePlot();
//# sourceMappingURL=vtkvolume.js.map