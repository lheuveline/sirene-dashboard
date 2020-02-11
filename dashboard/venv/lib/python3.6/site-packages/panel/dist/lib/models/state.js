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
var view_1 = require("@bokehjs/core/view");
var array_1 = require("@bokehjs/core/util/array");
var model_1 = require("@bokehjs/model");
var receiver_1 = require("@bokehjs/protocol/receiver");
function get_json(file, callback) {
    var xobj = new XMLHttpRequest();
    xobj.overrideMimeType("application/json");
    xobj.open('GET', file, true);
    xobj.onreadystatechange = function () {
        if (xobj.readyState == 4 && xobj.status == 200) {
            callback(xobj.responseText);
        }
    };
    xobj.send(null);
}
var StateView = /** @class */ (function (_super) {
    __extends(StateView, _super);
    function StateView() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    StateView.prototype.renderTo = function () {
    };
    StateView.__name__ = "StateView";
    return StateView;
}(view_1.View));
exports.StateView = StateView;
var State = /** @class */ (function (_super) {
    __extends(State, _super);
    function State(attrs) {
        var _this = _super.call(this, attrs) || this;
        _this._receiver = new receiver_1.Receiver();
        _this._cache = {};
        return _this;
    }
    State.prototype.apply_state = function (state) {
        this._receiver.consume(state.header);
        this._receiver.consume(state.metadata);
        this._receiver.consume(state.content);
        if (this._receiver.message && this.document) {
            this.document.apply_json_patch(this._receiver.message.content);
        }
    };
    State.prototype._receive_json = function (result, path) {
        var state = JSON.parse(result);
        this._cache[path] = state;
        var current = this.state;
        for (var _i = 0, _a = this.values; _i < _a.length; _i++) {
            var i = _a[_i];
            current = current[i];
        }
        if (current === path)
            this.apply_state(state);
        else if (this._cache[current])
            this.apply_state(this._cache[current]);
    };
    State.prototype.set_state = function (widget, value) {
        var _this = this;
        var values = array_1.copy(this.values);
        var index = this.widgets[widget.id];
        values[index] = value;
        var state = this.state;
        for (var _i = 0, values_1 = values; _i < values_1.length; _i++) {
            var i = values_1[_i];
            state = state[i];
        }
        this.values = values;
        if (this.json) {
            if (this._cache[state]) {
                this.apply_state(this._cache[state]);
            }
            else {
                get_json(state, function (result) { return _this._receive_json(result, state); });
            }
        }
        else {
            this.apply_state(state);
        }
    };
    State.init_State = function () {
        this.prototype.default_view = StateView;
        this.define({
            json: [p.Boolean, false],
            state: [p.Any, {}],
            widgets: [p.Any, {}],
            values: [p.Any, []],
        });
    };
    State.__name__ = "State";
    State.__module__ = "panel.models.state";
    return State;
}(model_1.Model));
exports.State = State;
State.init_State();
//# sourceMappingURL=state.js.map