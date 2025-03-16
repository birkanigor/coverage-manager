import log4js from 'log4js';
import { EnvReader } from './env';
const envReader = new EnvReader();

const logger = log4js.getLogger();

log4js.configure({
            appenders: {
                out: { type: "stdout" },
                console: { type: "console" },
                app: {
                    type: "file",
                    filename: "log/application.log",
                    maxLogSize: 20488 * 1024, // 1024 = 1KB
                    backups: 10,
                }
            },
            categories: {
                default: { appenders: ["app", "console"], level: "debug" },
                custom: { appenders: ["app", "console"], level: "debug" },
            },
        });

logger.level = envReader.getValue("LOG_LEVEL");

export default logger; // Export the logger instance