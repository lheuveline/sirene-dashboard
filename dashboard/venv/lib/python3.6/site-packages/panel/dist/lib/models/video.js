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
var layout_1 = require("./layout");
var VideoView = /** @class */ (function (_super) {
    __extends(VideoView, _super);
    function VideoView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    VideoView.prototype.initialize = function () {
        _super.prototype.initialize.call(this);
        this._blocked = false;
        this._setting = false;
        this._time = Date.now();
    };
    VideoView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.loop.change, function () { return _this.set_loop(); });
        this.connect(this.model.properties.paused.change, function () { return _this.set_paused(); });
        this.connect(this.model.properties.time.change, function () { return _this.set_time(); });
        this.connect(this.model.properties.value.change, function () { return _this.set_value(); });
        this.connect(this.model.properties.volume.change, function () { return _this.set_volume(); });
    };
    VideoView.prototype.render = function () {
        var _this = this;
        _super.prototype.render.call(this);
        this.videoEl = document.createElement('video');
        if (!this.model.sizing_mode || this.model.sizing_mode === 'fixed') {
            if (this.model.height)
                this.videoEl.height = this.model.height;
            if (this.model.width)
                this.videoEl.width = this.model.width;
        }
        this.videoEl.style.objectFit = 'fill';
        this.videoEl.style.minWidth = '100%';
        this.videoEl.style.minHeight = '100%';
        this.videoEl.controls = true;
        this.videoEl.src = this.model.value;
        this.videoEl.currentTime = this.model.time;
        this.videoEl.loop = this.model.loop;
        if (this.model.volume != null)
            this.videoEl.volume = this.model.volume / 100;
        else
            this.model.volume = this.videoEl.volume * 100;
        this.videoEl.onpause = function () { return _this.model.paused = true; };
        this.videoEl.onplay = function () { return _this.model.paused = false; };
        this.videoEl.ontimeupdate = function () { return _this.update_time(_this); };
        this.videoEl.onvolumechange = function () { return _this.update_volume(_this); };
        this.el.appendChild(this.videoEl);
        if (!this.model.paused)
            this.videoEl.play();
    };
    VideoView.prototype.update_time = function (view) {
        if (view._setting) {
            view._setting = false;
            return;
        }
        if ((Date.now() - view._time) < view.model.throttle)
            return;
        view._blocked = true;
        view.model.time = view.videoEl.currentTime;
        view._time = Date.now();
    };
    VideoView.prototype.update_volume = function (view) {
        if (view._setting) {
            view._setting = false;
            return;
        }
        view._blocked = true;
        view.model.volume = view.videoEl.volume * 100;
    };
    VideoView.prototype.set_loop = function () {
        this.videoEl.loop = this.model.loop;
    };
    VideoView.prototype.set_paused = function () {
        if (!this.videoEl.paused && this.model.paused)
            this.videoEl.pause();
        if (this.videoEl.paused && !this.model.paused)
            this.videoEl.play();
    };
    VideoView.prototype.set_volume = function () {
        if (this._blocked) {
            this._blocked = false;
            return;
        }
        this._setting = true;
        if (this.model.volume != null)
            this.videoEl.volume = this.model.volume / 100;
    };
    VideoView.prototype.set_time = function () {
        if (this._blocked) {
            this._blocked = false;
            return;
        }
        this._setting = true;
        this.videoEl.currentTime = this.model.time;
    };
    VideoView.prototype.set_value = function () {
        this.videoEl.src = this.model.value;
    };
    VideoView.__name__ = "VideoView";
    return VideoView;
}(layout_1.PanelHTMLBoxView));
exports.VideoView = VideoView;
var Video = /** @class */ (function (_super) {
    __extends(Video, _super);
    function Video(attrs) {
        return _super.call(this, attrs) || this;
    }
    Video.init_Video = function () {
        this.prototype.default_view = VideoView;
        this.define({
            loop: [p.Boolean, false],
            paused: [p.Boolean, true],
            time: [p.Number, 0],
            throttle: [p.Number, 250],
            value: [p.Any, ''],
            volume: [p.Number, null],
        });
    };
    Video.__name__ = "Video";
    Video.__module__ = "panel.models.widgets";
    return Video;
}(html_box_1.HTMLBox));
exports.Video = Video;
Video.init_Video();
//# sourceMappingURL=video.js.map