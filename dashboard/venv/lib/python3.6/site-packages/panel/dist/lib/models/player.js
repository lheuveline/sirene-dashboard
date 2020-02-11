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
var dom_1 = require("@bokehjs/core/dom");
var widget_1 = require("@bokehjs/models/widgets/widget");
var PlayerView = /** @class */ (function (_super) {
    __extends(PlayerView, _super);
    function PlayerView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    PlayerView.prototype.connect_signals = function () {
        var _this = this;
        _super.prototype.connect_signals.call(this);
        this.connect(this.model.change, function () { return _this.render(); });
        this.connect(this.model.properties.value.change, function () { return _this.render(); });
        this.connect(this.model.properties.loop_policy.change, function () { return _this.set_loop_state(_this.model.loop_policy); });
    };
    PlayerView.prototype.get_height = function () {
        return 250;
    };
    PlayerView.prototype.render = function () {
        var _this = this;
        if (this.sliderEl == null) {
            _super.prototype.render.call(this);
        }
        else {
            this.sliderEl.style.width = "{this.model.width}px";
            this.sliderEl.min = String(this.model.start);
            this.sliderEl.max = String(this.model.end);
            this.sliderEl.value = String(this.model.value);
            return;
        }
        // Slider
        this.sliderEl = document.createElement('input');
        this.sliderEl.setAttribute("type", "range");
        this.sliderEl.style.width = this.model.width + 'px';
        this.sliderEl.value = String(this.model.value);
        this.sliderEl.min = String(this.model.start);
        this.sliderEl.max = String(this.model.end);
        this.sliderEl.onchange = function (ev) { return _this.set_frame(parseInt(ev.target.value)); };
        // Buttons
        var button_div = dom_1.div();
        button_div.style.cssText = "margin: 0 auto; display: table; padding: 5px";
        var button_style = "text-align: center; min-width: 40px; margin: 2px";
        var slower = document.createElement('button');
        slower.style.cssText = "text-align: center; min-width: 20px";
        slower.appendChild(document.createTextNode('–'));
        slower.onclick = function () { return _this.slower(); };
        button_div.appendChild(slower);
        var first = document.createElement('button');
        first.style.cssText = button_style;
        first.appendChild(document.createTextNode('\u275a\u25c0\u25c0'));
        first.onclick = function () { return _this.first_frame(); };
        button_div.appendChild(first);
        var previous = document.createElement('button');
        previous.style.cssText = button_style;
        previous.appendChild(document.createTextNode('\u275a\u25c0'));
        previous.onclick = function () { return _this.previous_frame(); };
        button_div.appendChild(previous);
        var reverse = document.createElement('button');
        reverse.style.cssText = button_style;
        reverse.appendChild(document.createTextNode('\u25c0'));
        reverse.onclick = function () { return _this.reverse_animation(); };
        button_div.appendChild(reverse);
        var pause = document.createElement('button');
        pause.style.cssText = button_style;
        pause.appendChild(document.createTextNode('\u275a\u275a'));
        pause.onclick = function () { return _this.pause_animation(); };
        button_div.appendChild(pause);
        var play = document.createElement('button');
        play.style.cssText = button_style;
        play.appendChild(document.createTextNode('\u25b6'));
        play.onclick = function () { return _this.play_animation(); };
        button_div.appendChild(play);
        var next = document.createElement('button');
        next.style.cssText = button_style;
        next.appendChild(document.createTextNode('\u25b6\u275a'));
        next.onclick = function () { return _this.next_frame(); };
        button_div.appendChild(next);
        var last = document.createElement('button');
        last.style.cssText = button_style;
        last.appendChild(document.createTextNode('\u25b6\u25b6\u275a'));
        last.onclick = function () { return _this.last_frame(); };
        button_div.appendChild(last);
        var faster = document.createElement('button');
        faster.style.cssText = "text-align: center; min-width: 20px";
        faster.appendChild(document.createTextNode('+'));
        faster.onclick = function () { return _this.faster(); };
        button_div.appendChild(faster);
        // Loop control
        this.loop_state = document.createElement('form');
        this.loop_state.style.cssText = "margin: 0 auto; display: table";
        var once = document.createElement('input');
        once.type = "radio";
        once.value = "once";
        once.name = "state";
        var once_label = document.createElement('label');
        once_label.innerHTML = "Once";
        once_label.style.cssText = "padding: 0 10px 0 5px; user-select:none;";
        var loop = document.createElement('input');
        loop.setAttribute("type", "radio");
        loop.setAttribute("value", "loop");
        loop.setAttribute("name", "state");
        var loop_label = document.createElement('label');
        loop_label.innerHTML = "Loop";
        loop_label.style.cssText = "padding: 0 10px 0 5px; user-select:none;";
        var reflect = document.createElement('input');
        reflect.setAttribute("type", "radio");
        reflect.setAttribute("value", "reflect");
        reflect.setAttribute("name", "state");
        var reflect_label = document.createElement('label');
        reflect_label.innerHTML = "Reflect";
        reflect_label.style.cssText = "padding: 0 10px 0 5px; user-select:none;";
        if (this.model.loop_policy == "once")
            once.checked = true;
        else if (this.model.loop_policy == "loop")
            loop.checked = true;
        else
            reflect.checked = true;
        // Compose everything
        this.loop_state.appendChild(once);
        this.loop_state.appendChild(once_label);
        this.loop_state.appendChild(loop);
        this.loop_state.appendChild(loop_label);
        this.loop_state.appendChild(reflect);
        this.loop_state.appendChild(reflect_label);
        this.el.appendChild(this.sliderEl);
        this.el.appendChild(button_div);
        this.el.appendChild(this.loop_state);
    };
    PlayerView.prototype.set_frame = function (frame) {
        if (this.model.value != frame)
            this.model.value = frame;
        if (this.sliderEl.value != String(frame))
            this.sliderEl.value = String(frame);
    };
    PlayerView.prototype.get_loop_state = function () {
        var button_group = this.loop_state.state;
        for (var i = 0; i < button_group.length; i++) {
            var button = button_group[i];
            if (button.checked)
                return button.value;
        }
        return "once";
    };
    PlayerView.prototype.set_loop_state = function (state) {
        var button_group = this.loop_state.state;
        for (var i = 0; i < button_group.length; i++) {
            var button = button_group[i];
            if (button.value == state)
                button.checked = true;
        }
    };
    PlayerView.prototype.next_frame = function () {
        this.set_frame(Math.min(this.model.end, this.model.value + this.model.step));
    };
    PlayerView.prototype.previous_frame = function () {
        this.set_frame(Math.max(this.model.start, this.model.value - this.model.step));
    };
    PlayerView.prototype.first_frame = function () {
        this.set_frame(this.model.start);
    };
    PlayerView.prototype.last_frame = function () {
        this.set_frame(this.model.end);
    };
    PlayerView.prototype.slower = function () {
        this.model.interval = Math.round(this.model.interval / 0.7);
        if (this.model.direction > 0)
            this.play_animation();
        else if (this.model.direction < 0)
            this.reverse_animation();
    };
    PlayerView.prototype.faster = function () {
        this.model.interval = Math.round(this.model.interval * 0.7);
        if (this.model.direction > 0)
            this.play_animation();
        else if (this.model.direction < 0)
            this.reverse_animation();
    };
    PlayerView.prototype.anim_step_forward = function () {
        if (this.model.value < this.model.end) {
            this.next_frame();
        }
        else {
            var loop_state = this.get_loop_state();
            if (loop_state == "loop") {
                this.first_frame();
            }
            else if (loop_state == "reflect") {
                this.last_frame();
                this.reverse_animation();
            }
            else {
                this.pause_animation();
                this.last_frame();
            }
        }
    };
    PlayerView.prototype.anim_step_reverse = function () {
        if (this.model.value > this.model.start) {
            this.previous_frame();
        }
        else {
            var loop_state = this.get_loop_state();
            if (loop_state == "loop") {
                this.last_frame();
            }
            else if (loop_state == "reflect") {
                this.first_frame();
                this.play_animation();
            }
            else {
                this.pause_animation();
                this.first_frame();
            }
        }
    };
    PlayerView.prototype.pause_animation = function () {
        this.model.direction = 0;
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    };
    PlayerView.prototype.play_animation = function () {
        var _this = this;
        this.pause_animation();
        this.model.direction = 1;
        if (!this.timer)
            this.timer = setInterval(function () { return _this.anim_step_forward(); }, this.model.interval);
    };
    PlayerView.prototype.reverse_animation = function () {
        var _this = this;
        this.pause_animation();
        this.model.direction = -1;
        if (!this.timer)
            this.timer = setInterval(function () { return _this.anim_step_reverse(); }, this.model.interval);
    };
    PlayerView.__name__ = "PlayerView";
    return PlayerView;
}(widget_1.WidgetView));
exports.PlayerView = PlayerView;
var Player = /** @class */ (function (_super) {
    __extends(Player, _super);
    function Player(attrs) {
        return _super.call(this, attrs) || this;
    }
    Player.init_Player = function () {
        this.prototype.default_view = PlayerView;
        this.define({
            direction: [p.Number, 0],
            interval: [p.Number, 500],
            start: [p.Number,],
            end: [p.Number,],
            step: [p.Number, 1],
            loop_policy: [p.Any, "once"],
            value: [p.Any, 0],
        });
        this.override({ width: 400 });
    };
    Player.__name__ = "Player";
    Player.__module__ = "panel.models.widgets";
    return Player;
}(widget_1.Widget));
exports.Player = Player;
Player.init_Player();
//# sourceMappingURL=player.js.map