import express, { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { sessions, SECRET_KEY } from '../utils/constants';
import logger from "../app.logger";

export class AuthMiddleware  {

    authenticate = (req: express.Request, res: express.Response, next: express.NextFunction) => {
        try {

            const token = req.headers.authorization?.split(' ')[1];
            if (!token)  {
                logger.error('No token provided . Access denied');
                res.status(403).json({ error: 'Access denied' });
                return;
            }

            const decoded = jwt.verify(token, SECRET_KEY) as { sessionId: string; ip: string; appId: string };
            if (!sessions[decoded.sessionId] || sessions[decoded.sessionId].ip !== req.ip && sessions[decoded.sessionId].appId !== req.cookies.tab_session) {
                logger.error('Invalid token . Access denied');
                 res.status(403).json({ error: 'Invalid session' });
                 return
            }

            next();
        } catch (err) {
            logger.error('Invalid token');
            res.status(403).json({ error: 'Invalid token' });
            return;
        }
    };

    logRequestMiddleware = (req: express.Request, res: express.Response, next: express.NextFunction) => {
        logger.debug(`Received a ${req.method} request for ${req.url}`);
        next(); // Pass control to the next middleware or route handler
    };
}

