import dotenv from "dotenv";
import * as path from 'path';

export class EnvReader {
    private envPath: string;

    constructor(envPath: string = path.resolve(process.cwd(), process.env.ENV_FILE || '.env')) {
        this.envPath = envPath;
        dotenv.config({ path: envPath });
    }

    private getEnvVar = (key: string): string | null => {
        return process.env[key] ?? null;
    };

    public getValue(valueName: string): string  {
        return <string>this.getEnvVar(valueName);
    }
}
