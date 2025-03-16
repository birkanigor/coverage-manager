import { Request, Response } from 'express';
import { QueryResultRow} from 'pg';
import {PostgresQueryRunner} from "../dal"
import logger from "../app.logger";

export class DataController {
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)
    getData = async (req: Request, res: Response) => {
        const resData: QueryResultRow[] = await this.postgresQueryRunner.executeQuery('select * from cm_data.t_operator_info',[],true)
        res.json({value1: 1, value2: resData});
    };
}
