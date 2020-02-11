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
var VideoStreamView = /** @class */ (function (_super) {
    __extends(VideoStreamView, _super);
    function VideoStreamView() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this.constraints = {
            'audio': false,
            'video': true
        };
        return _this;
    }
    VideoStreamView.prototype.initialize = function () {
        _super.prototype.initialize.call(this);
        if (this.model.timeout !== null)
            this.set_timeout();
    };
    VideoStreamView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.timeout.change, function () { return _this.set_timeout(); });
        this.connect(this.model.properties.snapshot.change, function () { return _this.snapshot(); });
        this.connect(this.model.properties.paused.change, function () { return _this.pause(); });
    };
    VideoStreamView.prototype.pause = function () {
        if (this.model.paused) {
            if (this.timer != null) {
                clearInterval(this.timer);
                this.timer = null;
            }
            this.videoEl.pause();
        }
        this.set_timeout();
    };
    VideoStreamView.prototype.set_timeout = function () {
        var _this = this;
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
        if (this.model.timeout > 0)
            this.timer = setInterval(function () { return _this.snapshot(); }, this.model.timeout);
    };
    VideoStreamView.prototype.snapshot = function () {
        this.canvasEl.width = this.videoEl.videoWidth;
        this.canvasEl.height = this.videoEl.videoHeight;
        var context = this.canvasEl.getContext('2d');
        if (context)
            context.drawImage(this.videoEl, 0, 0, this.canvasEl.width, this.canvasEl.height);
        this.model.value = this.canvasEl.toDataURL("image/" + this.model.format, 0.95);
    };
    VideoStreamView.prototype.remove = function () {
        _super.prototype.remove.call(this);
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    };
    VideoStreamView.prototype.render = function () {
        var _this = this;
        _super.prototype.render.call(this);
        if (this.videoEl)
            return;
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
        this.canvasEl = document.createElement('canvas');
        this.el.appendChild(this.videoEl);
        if (navigator.mediaDevices.getUserMedia) {
            navigator.mediaDevices.getUserMedia(this.constraints)
                .then(function (stream) {
                _this.videoEl.srcObject = stream;
                if (!_this.model.paused) {
                    _this.videoEl.play();
                }
            })
                .catch(console.error);
        }
    };
    VideoStreamView.__name__ = "VideoStreamView";
    return VideoStreamView;
}(layout_1.PanelHTMLBoxView));
exports.VideoStreamView = VideoStreamView;
var VideoStream = /** @class */ (function (_super) {
    __extends(VideoStream, _super);
    function VideoStream(attrs) {
        return _super.call(this, attrs) || this;
    }
    VideoStream.init_VideoStream = function () {
        this.prototype.default_view = VideoStreamView;
        this.define({
            format: [p.String, 'png'],
            paused: [p.Boolean, false],
            snapshot: [p.Boolean, false],
            timeout: [p.Number, 0],
            value: [p.Any,]
        });
        this.override({
            height: 240,
            width: 320
        });
    };
    VideoStream.__name__ = "VideoStream";
    VideoStream.__module__ = "panel.models.widgets";
    return VideoStream;
}(html_box_1.HTMLBox));
exports.VideoStream = VideoStream;
VideoStream.init_VideoStream();
//# sourceMappingURL=videostream.js.map