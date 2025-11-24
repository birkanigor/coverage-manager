import express from 'express';
import bodyParser from 'body-parser';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import authRoutes from './routes/authRoutes';
import dataRoutes from './routes/dataRoutes';
import confRoutes from "./routes/confRoutes";
import uploadRoutes from "./routes/uploadRoutes"
import screenRoutes from "./routes/screensRoutes"
import {AuthMiddleware} from './middleware/authMiddleware'
import { EnvReader } from './env';
import logger from "./app.logger";

const app = express();
const envReader = new EnvReader();
const PORT:number = Number(envReader.getValue('APP_PORT')) || 3001;

const authMiddleware = new AuthMiddleware();

// Middleware
app.use(cookieParser());
app.use(cors());
app.use(bodyParser.json({ limit: '50mb' }));

// Routes
app.use('/', authMiddleware.logRequestMiddleware);
app.use('/auth', authRoutes);
app.use('/data', dataRoutes);
app.use('/conf', confRoutes);
app.use('/upload', uploadRoutes);
app.use('/screen', screenRoutes)

app.listen(PORT, () => {
    return logger.debug(`Express is listening at http://localhost:${PORT} `);
});