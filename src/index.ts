import Queue from './Queue';

// Exportar como CommonJS si está en ese entorno
if (typeof module !== 'undefined' && module.exports) {
    module.exports = Queue;
}

// Exportar como ESM
export default Queue;