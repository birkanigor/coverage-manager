import { Request, Response } from 'express';
import { Readable } from "stream";
import {PostgresQueryRunner} from "../dal"
import logger from "../app.logger";

export class UploadController{
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    getDataLoadersConf = async (req: Request, res: Response) => {
        try{
            logger.debug(`getDataLoadersConf API called`)
            const query = `select t1.id, t2.imsi_donor_name , data_set_name, temp_table_name
                from cm_conf.t_data_etl_conf t1 join cm_conf.t_imsi_donors t2 
                on t1.imsi_donor_id = t2.id
                order by t2.id`;
            const {rows, columns } = await this.postgresQueryRunner.executeQuery(query, [], true)
            res.json({status: 'SUCCESS', data: rows, columns, message: ''});
        }catch(error){            
            logger.error(`getDataLoadersConf API failed . Error : ${error}`)
            res.json({status: 'FAIL', data: null, columns: null, message: 'DB Error'});  
        }          
    }

    uploadData = async (req: Request, res: Response) => {
        try{
            const { data, tableName } = req.body;
            const csvBuffer = Buffer.from(data, "base64");
            const csvString = csvBuffer.toString("utf-8");
            logger.debug(`uploadData API called , data : ${data}`);
            
            const csvStream = Readable.from([csvString]);
            const result = await this.postgresQueryRunner.copyCsvStreamToTable(csvStream, tableName, true)

            res.json({status: result ? 'SUCCESS': 'FAIL',  message: ''});
        }catch(error){
            logger.error(`uploadData API failed . Error : ${error}`)
            res.json({status: 'FAIL', data: null, columns: null, message: 'DB Error'});  
        }

    }

    updateData = async (req: Request, res: Response) => {
        const { tableName , columnsList, valuesList, rowId} = req.body;
        logger.debug(`updateData API called . tableName : ${tableName}, columnsList : ${columnsList}, valuesList : ${valuesList}, rowId : ${rowId}`);

        try{       
            const setClauses = columnsList.map((col, index) => `"${col}" = $${index + 1}`);

            const query = `
                UPDATE "${tableName}"
                SET ${setClauses.join(", ")}
                WHERE "id" = $${columnsList.length + 1}
                returning id
            `;

            const {rows, columns } = await this.postgresQueryRunner.executeQuery(query, [...valuesList, rowId], true)
            res.json({status: 'SUCCESS', data: rows, columns, message: ''});
        }catch(error) {
            logger.error(`updateData error : ${error}`)
            res.json({status: 'FAIL', data:null, columns: null, message: 'DB Error'})
        }
    }

    getImsiDonorData  = async (req: Request, res: Response) => {
        const { tableName } = req.body;
        try{
            logger.debug(`getImsiDonorData API called. tableName : ${tableName}`)
            const query = `select * from ${tableName}`;
            const {rows, columns } = await this.postgresQueryRunner.executeQuery(query, [], true)
            res.json({status: 'SUCCESS', data: rows, columns, message: ''});
        }catch(error){            
            logger.error(`getImsiDonorData API failed . Error : ${error}`)
            res.json({status: 'FAIL', data: null, columns: null, message: 'DB Error'});  
        }   
    }
}

