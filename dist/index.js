"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Queue_1 = require("./Queue");
// Exportar como CommonJS si está en ese entorno
if (typeof module !== 'undefined' && module.exports) {
    module.exports = Queue_1.default;
}
// Exportar como ESM
exports.default = Queue_1.default;
