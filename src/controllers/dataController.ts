import { Request, Response } from 'express';

export const getData = (req: Request, res: Response) => {
    res.json({ value1: 1, value2: 'xyz' });
};
