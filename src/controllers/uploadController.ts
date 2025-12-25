import { Request, Response } from 'express';
import { Readable } from "stream";
import { PostgresQueryRunner } from "../dal"
import logger from "../app.logger";
import * as XLSX from 'xlsx';

export class UploadController {
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    getDataLoadersConf = async (req: Request, res: Response) => {
        try {
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
select t1.id, t2.imsi_donor_name , data_set_name, temp_table_name,permanent_table_name, versionsList , t4.columns
    from cm_conf.t_data_etl_conf t1 join cm_conf.t_imsi_donors t2                 
    on t1.imsi_donor_id = t2.id
    join table_info t4 on t1.id = t4.id
    left join versions t3 on t1.id = t3.etl_conf_id
    order by t2.id`;
            const { rows, columns } = await this.postgresQueryRunner.executeQuery(query, [], true)
            res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
        } catch (error) {
            logger.error(`getDataLoadersConf API failed . Error : ${error}`)
            res.json({ status: 'FAIL', data: null, columns: null, message: 'DB Error' });
        }
    }

    uploadData = async (req: Request, res: Response) => {
        try {
            const { data, tableName, tableId, versionName } = req.body;
            const csvBuffer = Buffer.from(data, "base64");
            const csvString = csvBuffer.toString("utf-8");
            logger.debug(`uploadData API called , data : ${data}`);

            const query = `insert into cm_conf.t_data_imsi_donor_versions (etl_conf_id , version_name , version_date) 
values ( $1, $2, current_date )
returning version_name || ' ( ' || version_date || ' )' new_version`
            const { rows, columns } = await this.postgresQueryRunner.executeQuery(query, [tableId, versionName], true)

            const csvStream = Readable.from([csvString]);
            const result = await this.postgresQueryRunner.copyCsvStreamToTable(csvStream, tableName, true)

            const transferFunctionQuery = `
            SELECT transfer_function_name
            FROM cm_conf.t_data_etl_conf
            WHERE id = $1
        `;

            const transferResult = await this.postgresQueryRunner.executeQuery(
                transferFunctionQuery,
                [tableId],
                true
            );

            if (!transferResult.rows?.length) {
                throw new Error(`No transfer function defined for tableId=${tableId}`);
            }

            const transferFunctionName = transferResult.rows[0].transfer_function_name;

            console.log(`transferFunctionName: ${transferFunctionName}`);
            

            // 4. הרצה דינמית של הפונקציה
            const transferFn = (this.postgresQueryRunner as any)[transferFunctionName];

            if (typeof transferFn !== "function") {
                throw new Error(
                    `Transfer function '${transferFunctionName}' does not exist`
                );
            }

            await transferFn.call(
                this.postgresQueryRunner,
                tableName,
                versionName,
                tableId
            );

            res.json({ status: result ? 'SUCCESS' : 'FAIL', message: '', data: rows });
        } catch (error) {
            logger.error(`uploadData API failed . Error : ${error}`)
            res.json({ status: 'FAIL', data: null, columns: null, message: 'DB Error' });
        }
    }

    updateData = async (req: Request, res: Response) => {
        const { tableName, columnsList, valuesList, rowId } = req.body;
        logger.debug(`updateData API called . tableName : ${tableName}, columnsList : ${columnsList}, valuesList : ${valuesList}, rowId : ${rowId}`);

        try {
            const setClauses = columnsList.map((col, index) => `"${col}" = $${index + 1}`);

            const query = `
                UPDATE "${tableName}"
                SET ${setClauses.join(", ")}
                WHERE "id" = $${columnsList.length + 1}
                returning id
            `;

            const { rows, columns } = await this.postgresQueryRunner.executeQuery(query, [...valuesList, rowId], true)
            res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
        } catch (error) {
            logger.error(`updateData error : ${error}`)
            res.json({ status: 'FAIL', data: null, columns: null, message: 'DB Error' })
        }
    }

    getImsiDonorData = async (req: Request, res: Response) => {
        const { tableName, version } = req.body;
        try {
            logger.debug(`getImsiDonorData API called. tableName : ${tableName}`)
            const query = `select * from ${tableName} where version_id = ${version} `;
            const { rows, columns } = await this.postgresQueryRunner.executeQuery(query, [], true)
            res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
        } catch (error) {
            logger.error(`getImsiDonorData API failed . Error : ${error}`)
            res.json({ status: 'FAIL', data: null, columns: null, message: 'DB Error' });
        }
    }

    uploadHotMobileExcel = async (req: Request, res: Response) => {
        try {
            const { data, tableName, tableId, versionName, skipRows } = req.body;
            logger.debug(`uploadHotMobileExcel API called`);

            // 1. Record version in t_data_imsi_donor_versions
            const versionQuery = `insert into cm_conf.t_data_imsi_donor_versions (etl_conf_id , version_name , version_date)
values ( $1, $2, current_date )
returning version_name || ' ( ' || version_date || ' )' new_version`;
            const { rows: versionRows } = await this.postgresQueryRunner.executeQuery(versionQuery, [tableId, versionName], true);

            // 2. Decode base64 to buffer
            const buffer = Buffer.from(data, "base64");

            // 3. Read workbook
            const workbook = XLSX.read(buffer, { type: "buffer" });
            const sheetName = workbook.SheetNames[0];
            const sheet = workbook.Sheets[sheetName];

            // 4. Convert sheet to array of arrays
            const rows = XLSX.utils.sheet_to_json(sheet, {
                header: 1,
                defval: null,
                blankrows: false,
            }) as any[][];

            // 5. Row indexes (0-based) - skipRows determines where data starts
            const TITLE_ROW_INDEX = (skipRows || 7);
            const DATA_START_INDEX = TITLE_ROW_INDEX + 1;

            if (rows.length <= DATA_START_INDEX) {
                logger.warn("No data rows found in Excel");
                res.json({ status: 'FAIL', data: null, columns: null, message: 'No data rows found in Excel' });
                return;
            }

            // 6. Extract data rows only
            const dataRows = rows
                .slice(DATA_START_INDEX)
                .filter(row =>
                    row.some(cell => cell !== null && String(cell).trim() !== "")
                );

            if (!dataRows.length) {
                logger.warn("All data rows are empty");
                res.json({ status: 'FAIL', data: null, columns: null, message: 'All data rows are empty' });
                return;
            }

            // 7. Get database client
            const client = await this.postgresQueryRunner['pool'].connect();

            try {
                await client.query("BEGIN");

                // Truncate temp table before inserting
                await client.query(`TRUNCATE TABLE ${tableName}`);

                const insertSql = `
                    INSERT INTO ${tableName} (
                        mccmnc,
                        plmn,
                        "operator",
                        country,
                        data_rate_euro_mb,
                        moc_mtc_sms_euro_min,
                        camel,
                        technology_2g_3g,
                        lte
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                `;

                for (const row of dataRows) {
                    await client.query(insertSql, [
                        row[0] ?? null, // mccmnc
                        row[1] ?? null, // plmn
                        row[2] ?? null, // operator
                        row[3] ?? null, // country
                        row[4] ?? null, // data_rate_euro_mb
                        row[5] ?? null, // moc_mtc_sms_euro_min
                        row[6] ?? null, // camel
                        row[7] ?? null, // technology_2g_3g
                        row[8] ?? null, // lte
                    ]);
                }

                await client.query("COMMIT");
                logger.info(`Inserted ${dataRows.length} rows into ${tableName}`);
            } catch (err) {
                await client.query("ROLLBACK");
                throw err;
            } finally {
                client.release();
            }

            // 8. Get and execute transfer function
            const transferFunctionQuery = `
                SELECT transfer_function_name
                FROM cm_conf.t_data_etl_conf
                WHERE id = $1
            `;

            const transferResult = await this.postgresQueryRunner.executeQuery(
                transferFunctionQuery,
                [tableId],
                true
            );

            if (!transferResult.rows?.length) {
                throw new Error(`No transfer function defined for tableId=${tableId}`);
            }

            const transferFunctionName = transferResult.rows[0].transfer_function_name;
            logger.debug(`transferFunctionName: ${transferFunctionName}`);

            // 9. Execute transfer function dynamically
            const transferFn = (this.postgresQueryRunner as any)[transferFunctionName];

            if (typeof transferFn !== "function") {
                throw new Error(
                    `Transfer function '${transferFunctionName}' does not exist`
                );
            }

            await transferFn.call(
                this.postgresQueryRunner,
                tableName,
                versionName,
                tableId
            );

            res.json({ status: 'SUCCESS', message: `Successfully uploaded ${dataRows.length} rows`, data: versionRows });
        } catch (error) {
            logger.error(`uploadHotMobileExcel API failed. Error : ${error}`);
            res.json({ status: 'FAIL', data: null, columns: null, message: 'DB Error' });
        }
    }
}

