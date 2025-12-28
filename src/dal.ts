import { Pool, QueryResult, QueryResultRow, PoolClient, Client } from 'pg';
import { from } from "pg-copy-streams";
import { Readable } from "stream";
import { EnvReader } from './env';
import { Logger as Log4jsLogger } from "log4js";

type QueryParams = any[];

export class PostgresQueryRunner {
    private pool: Pool;
    private log: Log4jsLogger;
    private client: Client;

    private typeMapping: { [key: number]: string } = {
        16: 'boolean',
        20: 'bigint',
        23: 'integer',
        25: 'text',
        700: 'real',
        701: 'double precision',
        1043: 'varchar',
        1082: 'date',
        1114: 'timestamp without time zone',
        1184: 'timestamp with time zone',
        // Add more OIDs if needed
    };
    postgresQueryRunner: any;

    constructor(log: Log4jsLogger) {
        const envReader = new EnvReader();
        this.log = log;

        const config = {
            user: envReader.getValue("DB_USER"),
            password: envReader.getValue("DB_PASSWORD"),
            host: envReader.getValue("DB_HOST"),
            port: parseInt(envReader.getValue("DB_PORT") || '5432', 10),
            database: envReader.getValue("DB_NAME"),
        }

        this.pool = new Pool(config);
        this.client = new Client(config)

        this.log.debug(`path : ${envReader.getEnvPath()}`)
        this.log.debug(JSON.stringify(this.pool.options));

        this.pool.on('connect', async (client: PoolClient) => {
            try {
                await client.query('SET search_path TO cm_conf, public');
            } catch (error) {
                this.log.error('Error setting search_path:', error);
            }
        });
    }

    /**
     * Executes a SQL query with optional parameters.
     * @param query - The SQL query string.
     * @param params - The optional array of query parameters.
     * @param loggable - The optional boolean flag to write the query to the log ( default ) or not
     * @returns A promise that resolves to the query result.
     */
    public async executeQuery<T extends QueryResultRow>(query: string, params: QueryParams = [], loggable: boolean = true): Promise<{ rows: T[], columns: { name: string; dataType: string }[] }> {
        try {
            if (loggable) {
                this.log.debug(`query: ${query} , params : ${JSON.stringify(params)}`);
            }

            const result: QueryResult = await this.pool.query<T>(query, params);

            const columns = result.fields.map(field => ({
                name: field.name,
                dataType: this.typeMapping[field.dataTypeID] || `Unknown(${field.dataTypeID})`
            }));

            return { rows: result.rows, columns };
        } catch (error) {
            this.log.error('Error executing query:', error);
            throw error;
        }
    }

    public async copyCsvStreamToTable(
        csvStream: Readable,
        tableName: string,
        truncateBeforeLoad = false
    ): Promise<boolean> {
        const client = await this.pool.connect(); // <-- borrow from pool
        try {
            this.log.log(`copyCsvStreamToTable started`);

            await client.query("BEGIN");

            if (truncateBeforeLoad) {
                await client.query(`TRUNCATE TABLE ${tableName}`);
            }

            const copySql = `COPY ${tableName} FROM STDIN WITH (FORMAT csv, HEADER true)`;
            this.log.log(`copySql : ${copySql}`);

            const copyStream = client.query(from(copySql));
            await new Promise<void>((resolve, reject) => {
                csvStream
                    .pipe(copyStream)
                    .on("finish", resolve)
                    .on("error", reject);
            });

            await client.query("COMMIT");
            this.log.log(`CSV successfully copied into ${tableName}`);
            return true;
        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during COPY:", err);
            return false;
        } finally {
            client.release(); // return to pool instead of end()
        }
    }

    // public async copyCsvStreamToTable(
    //     csvStream: Readable,
    //     tableName: string,
    //     truncateBeforeLoad = false
    // ): Promise<boolean> {
    //     await this.client.connect();
    //     try {
    //          this.log.log(`copyCsvStreamToTable started`)

    //         await this.client.query("BEGIN");

    //         if (truncateBeforeLoad) {
    //             await this.client.query(`TRUNCATE TABLE ${tableName}`);
    //         }

    //         const copySql = `COPY ${tableName}  FROM STDIN WITH (FORMAT csv, HEADER true)`;
    //         this.log.log(`copySql : ${copySql}`)

    //         const copyStream = this.client.query(from(copySql));
    //          this.log.log('copyStream done')
    //         await new Promise<void>((resolve, reject) => {
    //             csvStream
    //             .pipe(copyStream)
    //             .on("finish", resolve)
    //             .on("error", reject);
    //         });

    //         this.log.log(' Promise<void>((resolve, reject) done')

    //         await this.client.query("COMMIT");
    //         this.log.log(`CSV successfully copied into ${tableName}`);
    //         return true;
    //     } catch (err) {
    //         await this.client.query("ROLLBACK");
    //         this.log.error("Error during COPY:", err);
    //         return false;
    //     } finally {
    //         await this.client.end();
    //     }
    // }



    /**
     * Closes the database connection pool.
     */
    public async closeConnection(): Promise<void> {
        try {
            await this.pool.end();
            this.log.log('Database connection pool closed.');
        } catch (error) {
            this.log.error('Error closing connection pool:', error);
        }
    }



    public async appendToTele2Coverage(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect(); // borrow from pool

        try {
            this.log.log(`appendToTele2Coverage started`);
            await client.query("BEGIN");

            // 1. שליפת version_id
            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;

            const versionResult = await client.query(versionQuery, [
                versionName,
                tableId
            ]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            // 2. העתקה לטבלת יעד עם version_id
            const insertQuery = `
            INSERT INTO cm_data.t_tele2_coverage (
                region,
                country,
                operator_name,
                mgt_ccnc,
                mcc_mnc,
                tadig_code,
                gsm_out,
                gprs_out,
                tech_3g_out,
                camel_out,
                tech_4g_out,
                tech_5g_out,
                volte_out,
                lte_m_out,
                nbiot_out,
                nrtrde_out,
                steering,
                comments,
                psm_sup_lte_m,
                edrx_sup_lte_m,
                psm_sup_nbiot,
                edrx_sup_nbiot,
                version_id
            )
            SELECT
                region,
                country,
                operator_name,
                mgt_ccnc,
                mcc_mnc,
                tadig_code,
                gsm_out,
                gprs_out,
                tech_3g_out,
                camel_out,
                tech_4g_out,
                tech_5g_out,
                volte_out,
                lte_m_out,
                nbiot_out,
                nrtrde_out,
                steering,
                comments,
                psm_sup_lte_m,
                edrx_sup_lte_m,
                psm_sup_nbiot,
                edrx_sup_nbiot,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;

            await client.query(insertQuery, [versionId]);
            await client.query("COMMIT");

            this.log.log(
                `appendToTele2Coverage finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`
            );

            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToTele2Coverage:", err);
            return false;

        } finally {
            client.release(); // return to pool
        }
    }


    public async appendToTele2Updated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect(); // borrow from pool

        try {
            this.log.log(`appendToTele2Updated started`);

            await client.query("BEGIN");

            // 1. שליפת version_id
            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [versionName, tableId]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            // 2. העתקה לטבלת יעד עם version_id
            const insertQuery = `
            INSERT INTO cm_data.t_tele2_updated (
                region,
                country,
                network_name,
                tadig,
                moc_min,
                moc_eu_to_eu,
                mtc_min,
                sms,
                data_mb,
                access_fee_per_imsi_eur_month,
                network_comments,
                adjustment,
                version_id
            )
            SELECT
                region,
                country,
                network_name,
                tadig,
                moc_min,
                moc_eu_to_eu,
                mtc_min,
                sms,
                data_mb,
                access_fee_per_imsi_eur_month,
                network_comments,
                adjustment,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;
            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(`appendToTele2Updated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`);
            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToTele2Updated:", err);
            return false;

        } finally {
            client.release(); // return to pool
        }
    }


    public async appendToTele2VoiceUpdated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect(); // borrow from pool

        try {
            this.log.log(`appendToTele2VoiceUpdated started`);

            await client.query("BEGIN");

            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [versionName, tableId]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            const insertQuery = `
            INSERT INTO cm_data.t_tele2_voice_updated (
                region,
                country,
                network_name,
                plmnid,
                moc_min,
                mtc,
                sms,
                data_mb,
                data_increment,
                network_comments,
                version_id
            )
            SELECT
                region,
                country,
                network_name,
                plmnid,
                moc_min,
                mtc,
                sms,
                data_mb,
                data_increment,
                network_comments,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;
            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(`appendToTele2VoiceUpdated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`);
            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToTele2VoiceUpdated:", err);
            return false;

        } finally {
            client.release(); // return to pool
        }
    }



    public async appendToHotMobileUpdated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect(); // borrow from pool

        try {
            this.log.log(`appendToHotMobileUpdated started`);

            await client.query("BEGIN");

            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [versionName, tableId]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            const insertQuery = `
            INSERT INTO cm_data.t_hot_mobile_updated (
                mccmnc,
                plmn,
                operator,
                country,
                data_rate_euro_mb,
                moc_mtc_sms_euro_min,
                camel,
                technology_2g_3g,
                lte,
                version_id
            )
            SELECT
                mccmnc,
                plmn,
                operator,
                country,
                data_rate_euro_mb,
                moc_mtc_sms_euro_min,
                camel,
                technology_2g_3g,
                lte,
                $1::int4 AS version_id
            FROM ${sourceTableName}
            where trim(mccmnc) != ''
        `;
            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(`appendToHotMobileUpdated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`);
            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToHotMobileUpdated:", err);
            return false;

        } finally {
            client.release(); // return to pool
        }
    }



    public async appendToSparkleRoamingUpdated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect();

        try {
            this.log.log(`appendToSparkleRoamingUpdated started`);

            await client.query("BEGIN");

            // 1. שליפת version_id
            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [
                versionName,
                tableId
            ]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            // 2. העתקה לטבלת יעד – הכל varchar חוץ מ־version_id
            const insertQuery = `
            INSERT INTO cm_data.t_sparkle_roaming_updated (
                status,
                region,
                country,
                operator_name,
                country_code,
                mccmnc,
                plmno_code,
                technology_frequency,
                mtc_charging,
                gsm_roaming_outbound,
                camel_outbound,
                camel_phase_outbound,
                gprs_mms_outbound,
                umts_outbound,
                video_call,
                volte_outbound,
                lte_outbound,
                outbound_5g,
                lte_m_outbound,
                nbiot_outbound,
                lte_frequency,
                fallback_outbound,
                gsm_on_flight,
                camel_gsm_on_flight,
                camelph_out_gsm_on_flight,
                gsm_on_the_ship,
                camel_gsm_on_the_ship,
                camelph_out_gsm_on_the_ship,
                ira_update_launched_on,
                nrtrde_network_telecom_italia_itasi,
                nrtrde_commercial_date,
                version_id
            )
            SELECT
                status,
                region,
                country,
                operator_name,
                country_code,
                mccmnc,
                plmno_code,
                technology_frequency,
                mtc_charging,
                gsm_roaming_outbound,
                camel_outbound,
                camel_phase_outbound,
                gprs_mms_outbound,
                umts_outbound,
                video_call,
                volte_outbound,
                lte_outbound,
                outbound_5g,
                lte_m_outbound,
                nbiot_outbound,
                lte_frequency,
                fallback_outbound,
                gsm_on_flight,
                camel_gsm_on_flight,
                camelph_out_gsm_on_flight,
                gsm_on_the_ship,
                camel_gsm_on_the_ship,
                camelph_out_gsm_on_the_ship,
                ira_update_launched_on,
                nrtrde_network_telecom_italia_itasi,
                nrtrde_commercial_date,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;

            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(
                `appendToSparkleRoamingUpdated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`
            );

            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToSparkleRoamingUpdated:", err);
            return false;

        } finally {
            client.release();
        }
    }


    public async appendToSparklePriceUpdated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect();

        try {
            this.log.log(`appendToSparklePriceUpdated started`);

            await client.query("BEGIN");

            // 1. שליפת version_id
            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [versionName, tableId]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            // 2. העתקה לטבלת יעד עם version_id
            const insertQuery = `
            INSERT INTO cm_data.t_sparkle_price_updated (
                region,
                country,
                partner,
                tadig,
                moc_row_euro_per_minute,
                moc_local_euro_per_minute,
                mtc_euro_per_minute,
                sms_mt_euro_per_event,
                sms_mo_euro_per_event,
                data_roaming_euro_per_mbyte,
                volte_out_euro_per_mbyte,
                updated,
                status,
                version_id
            )
            SELECT
                region,
                country,
                partner,
                tadig,
                moc_row_euro_per_minute,
                moc_local_euro_per_minute,
                mtc_euro_per_minute,
                sms_mt_euro_per_event,
                sms_mo_euro_per_event,
                data_roaming_euro_per_mbyte,
                volte_out_euro_per_mbyte,
                updated,
                status,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;

            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(`appendToSparklePriceUpdated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`);
            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToSparklePriceUpdated:", err);
            return false;

        } finally {
            client.release();
        }
    }



    public async appendToBicsPriceUpdated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect();

        try {
            this.log.log(`appendToBicsPriceUpdated started`);

            await client.query("BEGIN");

            // 1. שליפת version_id
            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [versionName, tableId]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            // 2. העתקה לטבלת יעד עם version_id
            const insertQuery = `
            INSERT INTO cm_data.t_bics_price_updated (
                region,
                country_location,
                iso3,
                vpmn,
                tadig,
                voice_mo_local,
                voice_mo_call_inter_eu_zone1,
                voice_mo_calls_back_home,
                voice_mo_international_zone2,
                voice_mo_premium_satellites,
                voice_mt,
                sms_mo,
                data_mb,
                m2m_mrc_imsi,
                m2m_nb_iot_included,
                m2m_lte_m_included,
                nb_iot_mrc_imsi,
                lte_m_mrc_imsi,
                permanent_roaming_allowed_yes_no,
                charging_principles_voice,
                charging_principles_data,
                barring,
                sponsor,
                operator_comment,
                version_id
            )
            SELECT
                region,
                country_location,
                iso3,
                vpmn,
                tadig,
                voice_mo_local,
                voice_mo_call_inter_eu_zone1,
                voice_mo_calls_back_home,
                voice_mo_international_zone2,
                voice_mo_premium_satellites,
                voice_mt,
                sms_mo,
                data_mb,
                m2m_mrc_imsi,
                m2m_nb_iot_included,
                m2m_lte_m_included,
                nb_iot_mrc_imsi,
                lte_m_mrc_imsi,
                permanent_roaming_allowed_yes_no,
                charging_principles_voice,
                charging_principles_data,
                barring,
                sponsor,
                operator_comment,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;

            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(`appendToBicsPriceUpdated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`);
            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToBicsPriceUpdated:", err);
            return false;

        } finally {
            client.release();
        }
    }



    public async appendToBicsCoverageUpdated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect();

        try {
            this.log.log(`appendToBicsCoverageUpdated started`);

            await client.query("BEGIN");

            // 1. שליפת version_id
            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [versionName, tableId]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            // 2. העתקה לטבלת יעד עם version_id
            const insertQuery = `
            INSERT INTO cm_data.t_bics_coverage_updated (
                roaming_country,
                roaming_partner_name,
                hub_reference,
                serving_sponsor,
                current_status,
                customer_barring,
                bics_barring,
                sponsor_fra09_barring,
                remark,
                version_id
            )
            SELECT
                roaming_country,
                roaming_partner_name,
                hub_reference,
                serving_sponsor,
                current_status,
                customer_barring,
                bics_barring,
                sponsor_fra09_barring,
                remark,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;

            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(`appendToBicsCoverageUpdated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`);
            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToBicsCoverageUpdated:", err);
            return false;

        } finally {
            client.release();
        }
    }



    public async appendToBicsCoverageBandsUpdated(
        sourceTableName: string,
        versionName: string,
        tableId: number
    ): Promise<boolean> {

        const client = await this.pool.connect();

        try {
            this.log.log(`appendToBicsCoverageBandsUpdated started`);

            await client.query("BEGIN");

            // 1. שליפת version_id
            const versionQuery = `
            SELECT id
            FROM cm_conf.t_data_imsi_donor_versions
            WHERE version_name = $1 AND etl_conf_id = $2
            LIMIT 1
        `;
            const versionResult = await client.query(versionQuery, [versionName, tableId]);

            if (!versionResult.rows?.length) {
                throw new Error(
                    `Version not found. versionName=${versionName}, tableId=${tableId}`
                );
            }

            const versionId = versionResult.rows[0].id;

            // 2. העתקה לטבלת יעד
            const insertQuery = `
            INSERT INTO cm_data.t_bics_coverage_bands_updated (
                country_destination,
                operator_name,
                cc,
                nc,
                iso_code,
                tap_codes,
                mcc,
                mnc,
                sigos_mccmnc,
                fra09_gsm_launch,
                fra09_gprs_launch,
                fra09_3g_launch,
                fra09_camel_ph1_launch,
                fra09_camel_ph2_launch,
                fra09_lte_launch,
                fra09_lte_m_launch,
                edrx_lte_m,
                psm_lte_m,
                fra09_nb_iot_launch,
                edrx_nb_iot,
                psm_nb_iot,
                fra09_5g_nsa_launch,
                fra09_volte_launch,
                fra09_nrtrde_launch,
                barring_reference_bics,
                agtrange,
                gtrange,
                all_gt_changes,
                version_id
            )
            SELECT
                country_destination,
                operator_name,
                cc,
                nc,
                iso_code,
                tap_codes,
                mcc,
                mnc,
                sigos_mccmnc,
                fra09_gsm_launch,
                fra09_gprs_launch,
                fra09_3g_launch,
                fra09_camel_ph1_launch,
                fra09_camel_ph2_launch,
                fra09_lte_launch,
                fra09_lte_m_launch,
                edrx_lte_m,
                psm_lte_m,
                fra09_nb_iot_launch,
                edrx_nb_iot,
                psm_nb_iot,
                fra09_5g_nsa_launch,
                fra09_volte_launch,
                fra09_nrtrde_launch,
                barring_reference_bics,
                agtrange,
                gtrange,
                all_gt_changes,
                $1::int4 AS version_id
            FROM ${sourceTableName}
        `;

            await client.query(insertQuery, [versionId]);

            await client.query("COMMIT");

            this.log.log(`appendToBicsCoverageBandsUpdated finished successfully. sourceTable=${sourceTableName}, versionId=${versionId}`);
            return true;

        } catch (err) {
            await client.query("ROLLBACK");
            this.log.error("Error during appendToBicsCoverageBandsUpdated:", err);
            return false;

        } finally {
            client.release();
        }
    }

}
