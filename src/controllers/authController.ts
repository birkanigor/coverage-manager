import { Request, Response } from 'express';
import { Pool , QueryResultRow} from 'pg';
import jwt from 'jsonwebtoken';
import { sessions, SECRET_KEY, USER } from '../utils/constants';
import {PostgresQueryRunner} from "../dal"
import logger from "../app.logger";

export class AuthController {
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger);

    login = async (req: Request, res: Response) => {
        const {user_name, password} = req.body;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select  user_name, first_name, last_name, user_phone_number, user_email, t2.type_name , user_status\n' +
            'from cm_conf.t_cm_users t1 join cm_conf.t_cm_user_types t2 on t1.user_type = t2.id\n' +
            'where user_name = $1 \n' +
            'and user_password = crypt( $2, user_password )\n' +
            'and user_status = 1', [user_name, password], true);

        if (rows.length === 1) {
            const sessionId = `${Date.now()}-${Math.random()}`;
            const ip: string = req.ip || '';
            const tabId: string = Math.random().toString(36).slice(2, 11);

            const token = jwt.sign({sessionId, ip, tabId}, SECRET_KEY, {expiresIn: '1h'});
            sessions[sessionId] = {ip, appId: tabId};
            res.cookie("tab_session", tabId, {httpOnly: true});
            res.json({userData: rows , token});
        } else {
            res.status(401).json({error: 'Invalid credentials'});
        }
    };

    logout = (req: Request, res: Response) => {
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
}
