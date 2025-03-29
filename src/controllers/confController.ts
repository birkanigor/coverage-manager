import { Request, Response } from 'express';
import { QueryResultRow} from 'pg';
import {PostgresQueryRunner} from "../dal"
import logger from "../app.logger";

export class ConfController {
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    getTcpList = async (req: Request, res: Response) => {
        logger.debug(`getTcpList API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery('select id , tcp_name from cm_data.t_next_tcps order by id',[],true)
        res.json({status: 'SUCCESS', data: rows, columns, message: ''});
    };

    getPzCutOffPoints = async (req: Request, res: Response) => {
        const { id } = req.body;
        logger.debug(`getBapData API called , id : ${id}`)
        if(![1,2,3,4,5].includes(Number(id))){
            res.json({status: 'FAIL', data: [], message: 'Invalid TCP number'});
        }else{
            const { rows, columns } = await this.postgresQueryRunner.executeQuery(
                'select price_zone , cut_off_point\n' +
                'from cm_data.t_pz_cut_off_points\n' +
                'where tcp_id = $1',[id],true);
            res.json({status: 'SUCCESS', data: rows, columns, message: ''});
        }
    }
}