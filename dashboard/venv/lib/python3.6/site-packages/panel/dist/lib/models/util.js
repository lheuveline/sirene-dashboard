"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.get = function (obj, path, defaultValue) {
    if (defaultValue === void 0) { defaultValue = undefined; }
    var travel = function (regexp) {
        return String.prototype.split
            .call(path, regexp)
            .filter(Boolean)
            .reduce(function (res, key) { return (res !== null && res !== undefined ? res[key] : res); }, obj);
    };
    var result = travel(/[,[\]]+?/) || travel(/[,[\].]+?/);
    return result === undefined || result === obj ? defaultValue : result;
};
function throttle(func, timeFrame) {
    var lastTime = 0;
    return function () {
        var now = Number(new Date());
        if (now - lastTime >= timeFrame) {
            func();
            lastTime = now;
        }
    };
}
exports.throttle = throttle;
function deepCopy(obj) {
    var copy;
    // Handle the 3 simple types, and null or undefined
    if (null == obj || "object" != typeof obj)
        return obj;
    // Handle Array
    if (obj instanceof Array) {
        copy = [];
        for (var i = 0, len = obj.length; i < len; i++) {
            copy[i] = deepCopy(obj[i]);
        }
        return copy;
    }
    // Handle Object
    if (obj instanceof Object) {
        var copy_1 = {};
        for (var attr in obj) {
            var key = attr;
            if (obj.hasOwnProperty(key))
                copy_1[key] = deepCopy(obj[key]);
        }
        return copy_1;
    }
    throw new Error("Unable to copy obj! Its type isn't supported.");
}
exports.deepCopy = deepCopy;
function isPlainObject(obj) {
    return Object.prototype.toString.call(obj) === '[object Object]';
}
exports.isPlainObject = isPlainObject;
//# sourceMappingURL=util.js.map