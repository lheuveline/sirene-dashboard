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
var AudioView = /** @class */ (function (_super) {
    __extends(AudioView, _super);
    function AudioView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    AudioView.prototype.initialize = function () {
        _super.prototype.initialize.call(this);
        this._blocked = false;
        this._setting = false;
        this._time = Date.now();
    };
    AudioView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.properties.loop.change, function () { return _this.set_loop(); });
        this.connect(this.model.properties.paused.change, function () { return _this.set_paused(); });
        this.connect(this.model.properties.time.change, function () { return _this.set_time(); });
        this.connect(this.model.properties.value.change, function () { return _this.set_value(); });
        this.connect(this.model.properties.volume.change, function () { return _this.set_volume(); });
    };
    AudioView.prototype.render = function () {
        var _this = this;
        _super.prototype.render.call(this);
        this.audioEl = document.createElement('audio');
        this.audioEl.controls = true;
        this.audioEl.src = this.model.value;
        this.audioEl.currentTime = this.model.time;
        this.audioEl.loop = this.model.loop;
        if (this.model.volume != null)
            this.audioEl.volume = this.model.volume / 100;
        else
            this.model.volume = this.audioEl.volume * 100;
        this.audioEl.onpause = function () { return _this.model.paused = true; };
        this.audioEl.onplay = function () { return _this.model.paused = false; };
        this.audioEl.ontimeupdate = function () { return _this.update_time(_this); };
        this.audioEl.onvolumechange = function () { return _this.update_volume(_this); };
        this.el.appendChild(this.audioEl);
        if (!this.model.paused)
            this.audioEl.play();
    };
    AudioView.prototype.update_time = function (view) {
        if (view._setting) {
            view._setting = false;
            return;
        }
        if ((Date.now() - view._time) < view.model.throttle)
            return;
        view._blocked = true;
        view.model.time = view.audioEl.currentTime;
        view._time = Date.now();
    };
    AudioView.prototype.update_volume = function (view) {
        if (view._setting) {
            view._setting = false;
            return;
        }
        view._blocked = true;
        view.model.volume = view.audioEl.volume * 100;
    };
    AudioView.prototype.set_loop = function () {
        this.audioEl.loop = this.model.loop;
    };
    AudioView.prototype.set_paused = function () {
        if (!this.audioEl.paused && this.model.paused)
            this.audioEl.pause();
        if (this.audioEl.paused && !this.model.paused)
            this.audioEl.play();
    };
    AudioView.prototype.set_volume = function () {
        if (this._blocked) {
            this._blocked = false;
            return;
        }
        this._setting = true;
        if (this.model.volume != null) {
            this.audioEl.volume = this.model.volume / 100;
        }
    };
    AudioView.prototype.set_time = function () {
        if (this._blocked) {
            this._blocked = false;
            return;
        }
        this._setting = true;
        this.audioEl.currentTime = this.model.time;
    };
    AudioView.prototype.set_value = function () {
        this.audioEl.src = this.model.value;
    };
    AudioView.__name__ = "AudioView";
    return AudioView;
}(layout_1.PanelHTMLBoxView));
exports.AudioView = AudioView;
var Audio = /** @class */ (function (_super) {
    __extends(Audio, _super);
    function Audio(attrs) {
        return _super.call(this, attrs) || this;
    }
    Audio.init_Audio = function () {
        this.prototype.default_view = AudioView;
        this.define({
            loop: [p.Boolean, false],
            paused: [p.Boolean, true],
            time: [p.Number, 0],
            throttle: [p.Number, 250],
            value: [p.Any, ''],
            volume: [p.Number, null],
        });
    };
    Audio.__name__ = "Audio";
    Audio.__module__ = "panel.models.widgets";
    return Audio;
}(html_box_1.HTMLBox));
exports.Audio = Audio;
Audio.init_Audio();
//# sourceMappingURL=audio.js.map