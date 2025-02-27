// import express from 'express';
// import bodyParser from 'body-parser';
// import cors from 'cors';
// import authRoutes from './routes/authRoutes';
// import dataRoutes from './routes/dataRoutes';
//
// const app = express();
// const PORT: number = 3000;
//
// // Middleware
// app.use(cors());
// app.use(bodyParser.json());
//
// // Routes
// app.use('/auth', authRoutes);
// app.use('/data', dataRoutes);
//
// // Start Server
// app.listen(PORT, () => {
//     console.log(`Server running on port ${PORT}`);
// });

import express from 'express';
import bodyParser from 'body-parser';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import authRoutes from './routes/authRoutes';
import dataRoutes from './routes/dataRoutes';
import {AuthMiddleware} from './middleware/authMiddleware'

const app = express();
const port = 3000;
const authMiddleware = new AuthMiddleware()

// Middleware
app.use(cookieParser());
app.use(cors());
app.use(bodyParser.json());

// Routes
app.use('/', authMiddleware.logRequestMiddleware);
app.use('/auth', authRoutes);
app.use('/data', dataRoutes);

app.listen(port, () => {
    return console.log(`Express is listening at http://localhost:${port} `);
});