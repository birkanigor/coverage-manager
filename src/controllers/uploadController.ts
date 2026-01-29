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

//     uploadData = async (req: Request, res: Response) => {
//         try {
//             const { data, tableName, tableId, versionName } = req.body;
//             const csvBuffer = Buffer.from(data, "base64");
//             const csvString = csvBuffer.toString("utf-8");
//             logger.debug(`uploadData API called , data : ${data}`);

//             const query = `insert into cm_conf.t_data_imsi_donor_versions (etl_conf_id , version_name , version_date) 
// values ( $1, $2, current_date )
// returning version_name || ' ( ' || version_date || ' )' new_version`
//             const { rows, columns } = await this.postgresQueryRunner.executeQuery(query, [tableId, versionName], true)

//             const csvStream = Readable.from([csvString]);
//             const result = await this.postgresQueryRunner.copyCsvStreamToTable(csvStream, tableName, true)

//             const transferFunctionQuery = `
//             SELECT transfer_function_name
//             FROM cm_conf.t_data_etl_conf
//             WHERE id = $1
//         `;

//             const transferResult = await this.postgresQueryRunner.executeQuery(
//                 transferFunctionQuery,
//                 [tableId],
//                 true
//             );

//             if (!transferResult.rows?.length) {
//                 throw new Error(`No transfer function defined for tableId=${tableId}`);
//             }

//             const transferFunctionName = transferResult.rows[0].transfer_function_name;

//             console.log(`transferFunctionName: ${transferFunctionName}`);
            

//             // 4. הרצה דינמית של הפונקציה
//             const transferFn = (this.postgresQueryRunner as any)[transferFunctionName];

//             if (typeof transferFn !== "function") {
//                 throw new Error(
//                     `Transfer function '${transferFunctionName}' does not exist`
//                 );
//             }

//             await transferFn.call(
//                 this.postgresQueryRunner,
//                 tableName,
//                 versionName,
//                 tableId
//             );

//             res.json({ status: result ? 'SUCCESS' : 'FAIL', message: '', data: rows });
//         } catch (error) {
//             logger.error(`uploadData API failed . Error : ${error}`)
//             res.json({ status: 'FAIL', data: null, columns: null, message: 'DB Error' });
//         }
//     }

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

    updateTitle = async (req: Request, res: Response) => {
        const { subscreen_level_2_id, column_name, new_title_name } = req.body;
        logger.debug(`updateTitle API called. subscreen_level_2_id: ${subscreen_level_2_id}, column_name: ${column_name}, new_title_name: ${new_title_name}`);

        try {
            const query = `select cm_conf.fn_update_etl_column_comment($1, $2, $3)`;
            const { rows } = await this.postgresQueryRunner.executeQuery(query, [subscreen_level_2_id, column_name, new_title_name], true);

            const updatedColumnName = rows[0]?.fn_update_etl_column_comment;

            if (updatedColumnName === new_title_name) {
                res.json({ status: 'SUCCESS', data: updatedColumnName, message: '' });
            } else {
                res.json({ status: 'FAIL', data: -1, message: 'Update failed - returned value does not match' });
            }
        } catch (error) {
            logger.error(`updateTitle error: ${error}`);
            res.json({ status: 'FAIL', data: -1, message: 'DB Error' });
        }
    }

    uploadExcelFile = async (req: Request, res: Response) => {
        try {
            const { data, tableName, tableId, versionName, skipRows } = req.body;
            logger.debug(`uploadExcelFile API called for table: ${tableName}`);

            // 1. Record version in t_data_imsi_donor_versions
            const versionQuery = `insert into cm_conf.t_data_imsi_donor_versions (etl_conf_id , version_name , version_date)
values ( $1, $2, current_date )
returning id, version_name || ' ( ' || version_date || ' )' new_version`;
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
            logger.debug(`skipRows: ${skipRows}`);

            const TITLE_ROW_INDEX = skipRows;
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

                // 8. Dynamically get table columns from database
                const columnQuery = `
                    SELECT column_name, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = split_part($1, '.', 1)
                      AND table_name = split_part($1, '.', 2)
                      AND column_name != 'id'
                    ORDER BY ordinal_position
                `;

                const columnResult = await client.query(columnQuery, [tableName]);
                const tableColumns = columnResult.rows.map(row => row.column_name);

                if (!tableColumns.length) {
                    throw new Error(`No columns found for table ${tableName}`);
                }

                logger.debug(`Table columns: ${tableColumns.join(', ')}`);

                // 9. Build dynamic INSERT statement
                const columnsList = tableColumns.map(col => `"${col}"`).join(', ');
                const placeholders = tableColumns.map((_, idx) => `$${idx + 1}`).join(', ');

                const insertSql = `
                    INSERT INTO ${tableName} (${columnsList})
                    VALUES (${placeholders})
                `;

                logger.debug(`Insert SQL: ${insertSql}`);

                // 10. Insert rows with dynamic column mapping
                for (const row of dataRows) {
                    // Map Excel columns (0-indexed) to table columns
                    const values = tableColumns.map((_, idx) => {
                        const cellValue = row[idx];
                        return cellValue !== undefined && cellValue !== null ? cellValue : null;
                    });

                    await client.query(insertSql, values);
                }

                await client.query("COMMIT");
                logger.info(`Inserted ${dataRows.length} rows into ${tableName}`);
            } catch (err) {
                await client.query("ROLLBACK");
                throw err;
            } finally {
                client.release();
            }

            // 11. Get and execute transfer function
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

            // 12. Execute transfer function dynamically
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

