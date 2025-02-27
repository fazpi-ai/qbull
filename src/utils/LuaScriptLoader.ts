import { promises as fs } from 'fs';
import { join } from 'path';

export class LuaScriptLoader {
    private scripts: Map<string, string> = new Map();

    constructor(private scriptsDir: string) { }

    async loadScript(scriptName: string): Promise<void> {
        // Construir la ruta absoluta asumiendo que scriptsDir está dentro de 'src'
        const filePath = join(__dirname, '..', this.scriptsDir, `${scriptName}.lua`);
        try {
            const scriptContent = await fs.readFile(filePath, 'utf8');
            this.scripts.set(scriptName, scriptContent);
        } catch (error) {
            console.error(`Error loading script ${scriptName}:`, error);
        }
    }

    getScript(scriptName: string): string | undefined {
        return this.scripts.get(scriptName);
    }
}