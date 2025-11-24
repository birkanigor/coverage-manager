import { Request, Response } from 'express';
import { Readable } from "stream";
import {PostgresQueryRunner} from "../dal"
import logger from "../app.logger";

export class UploadController{
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    getDataLoadersConf = async (req: Request, res: Response) => {
        try{
            logger.debug(`getDataLoadersConf API called`)
            const query = `with 
versions as
	( select etl_conf_id , json_agg( json_build_object('name', version_name || '( ' || version_date || ' )', 'id', id)) versionsList 
	 from cm_conf.t_data_imsi_donor_versions
	 group by etl_conf_id),
table_info as
	(select t5.id, t2.relname table_name,
	 json_agg(json_build_object('column_name' , t1.attname, 'position', t1.attnum , 'data_type', format_type(t1.atttypid, t1.atttypmod), 'title', t4.description ) order by t1.attnum ) columns
	from pg_attribute t1
	join pg_class t2 on t1.attrelid = t2.oid
	join pg_namespace t3 on t2.relnamespace = t3.oid
	join cm_conf.t_data_etl_conf t5 on t2.relname =  split_part(t5.temp_table_name, '.', 2)
	left join pg_description t4 on t4.objoid = t1.attrelid  and t4.objsubid = t1.attnum
	where t3.nspname = 'cm_temp'
	  and t1.attnum > 0
	  and not t1.attisdropped
	group by t5.id, t2.relname )
select t1.id, t2.imsi_donor_name , data_set_name, temp_table_name, versionsList , t4.columns
    from cm_conf.t_data_etl_conf t1 join cm_conf.t_imsi_donors t2                 
    on t1.imsi_donor_id = t2.id
    join table_info t4 on t1.id = t4.id
    left join versions t3 on t1.id = t3.etl_conf_id
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
            const { data, tableName, tableId, versionName } = req.body;
            const csvBuffer = Buffer.from(data, "base64");
            const csvString = csvBuffer.toString("utf-8");
            logger.debug(`uploadData API called , data : ${data}`);

            const query = `insert into cm_conf.t_data_imsi_donor_versions (etl_conf_id , version_name , version_date) 
values ( $1, $2, current_date )
returning version_name || ' ( ' || version_date || ' )' new_version`
            const {rows, columns } = await this.postgresQueryRunner.executeQuery(query, [tableId, versionName], true)
            
            const csvStream = Readable.from([csvString]);
            const result = 'SUCCESS'//await this.postgresQueryRunner.copyCsvStreamToTable(csvStream, tableName, true)

            res.json({status: result ? 'SUCCESS': 'FAIL',  message: '', data: rows});
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

