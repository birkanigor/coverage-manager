import { Request, Response } from 'express';
import jwt from 'jsonwebtoken';
import { sessions, SECRET_KEY, USER } from '../utils/constants';

export const login = (req: Request, res: Response) => {
    const { user_name, password } = req.body;
    if (user_name === USER.user_name && password === USER.password) {
        const sessionId = `${Date.now()}-${Math.random()}`;
        const ip: string = req.ip || '';
        const tabId: string = Math.random().toString(36).slice(2, 11);

        const token = jwt.sign({ sessionId, ip, tabId }, SECRET_KEY, { expiresIn: '1h' });
        sessions[sessionId] = { ip, appId: tabId };
        res.cookie("tab_session", tabId, { httpOnly: true });
        res.json({ token });
    } else {
        res.status(401).json({ error: 'Invalid credentials' });
    }
};

export const logout = (req: Request, res: Response) => {
    const token = req.headers.authorization?.split(' ')[1];
    if (token) {
        try {
            const decoded = jwt.verify(token, SECRET_KEY) as { sessionId: string };
            delete sessions[decoded.sessionId];
            res.json({ message: 'Logged out' });
        } catch (err) {
            res.status(400).json({ error: 'Invalid token' });

            return;
        }
    }
    res.json({ message: 'Logged out successfully' });
};
