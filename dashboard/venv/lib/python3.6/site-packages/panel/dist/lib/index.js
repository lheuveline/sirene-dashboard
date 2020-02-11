"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
var Panel = __importStar(require("./models"));
exports.Panel = Panel;
var base_1 = require("@bokehjs/base");
base_1.register_models(Panel);
//# sourceMappingURL=index.js.map