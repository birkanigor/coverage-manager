import { Request, Response } from 'express';
import { PostgresQueryRunner } from "../dal"
import logger from "../app.logger";


export class MasterController {
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    getSavedVersions = async (req: Request, res: Response) => {
        logger.debug('getSavedVersions API called');

        const query = `
            SELECT id, version_name || ' ( ' || version_date || ' )' AS version_name
            FROM cm_conf.t_master_config
        `;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(query, [], true);

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getSavedVersionById = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getSavedVersionById API called with id: ${versionId}`);

        // Get the version IDs from t_master_config
        const configQuery = `
            SELECT
                tele2_coverage,
                tele2_updated,
                tele2_voice_updated,
                tim_sparkle_price_updated,
                tim_sparkle_roaming_updated,
                hot_mobile_updated,
                bics_coverage_bands_updated,
                bics_coverage_updated,
                bics_price_updated
            FROM cm_conf.t_master_config
            WHERE id = $1
        `;

        const { rows: configRows } = await this.postgresQueryRunner.executeQuery(configQuery, [versionId], true);

        if (configRows.length === 0) {
            logger.warn(`No config found for master_config_id: ${versionId}`);
            res.status(404).json({
                status: 'ERROR',
                message: 'Version not found',
                data: null
            });
            return;
        }

        // Convert config row to numbered object
        const versionIds = {
            1: configRows[0].tele2_coverage,
            2: configRows[0].tele2_updated,
            3: configRows[0].tele2_voice_updated,
            4: configRows[0].tim_sparkle_price_updated,
            5: configRows[0].tim_sparkle_roaming_updated,
            6: configRows[0].hot_mobile_updated,
            7: configRows[0].bics_coverage_bands_updated,
            8: configRows[0].bics_coverage_updated,
            9: configRows[0].bics_price_updated
        };

        // First, check if any data exists for this master_config_id
        const checkQuery = `SELECT COUNT(*) as count FROM cm_data.t_master_data WHERE master_config_id = $1`;
        const { rows: checkRows } = await this.postgresQueryRunner.executeQuery(checkQuery, [versionId], true);
        logger.debug(`Found ${checkRows[0].count} rows for master_config_id: ${versionId}`);

        const query = `
            SELECT
                plmno_code, mcc_mnc, region, country, operator_name, country_code, mgt,
                sparkle_coverage, hot_coverage, tele2_coverage, bics_coverage,
                sparkle_2g, sparkle_3g, sparkle_4g,
                hot_2g, hot_3g, hot_4g,
                tele2_2g, tele2_3g, tele2_4g,
                bics_2g, bics_3g, bics_4g,
                eprofile_3_tim, hot_zone, eprofile_2_tele2, eprofile_1_bics,
                tim_data_per_mb, tim_sms_mo, tim_voice_mo, tim_voice_mt,
                hot_data, hot_sms, hot_moc, hot_mtc,
                tele2_data, tele2_sms_mo, tele2_voice_mo, tele2_voice_mt,
                bics_data, bics_sms, bics_voice_mo, bics_voice_mt,
                imsi_donor_tcp1, profile1_pz, profile1_price, profile1_broadband,
                imsi_donor_tcp2, profile2_pz, profile2_price, profile2_broadband,
                imsi_donor_tcp3, profile3_pz, profile3_price, profile3_broadband,
                imsi_donor_tcp4, profile4_pz, profile4_price, profile4_broadband,
                imsi_donor_tcp5, profile5_pz, profile5_price, profile5_broadband,
                prr, blocked_countries,
                comments_profile_1, comments_profile_2, comments_profile_3, comments_profile_4, comments_profile_5
            FROM cm_data.t_master_data
            WHERE master_config_id = $1
        `;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(query, [versionId], true);

        if (rows.length === 0) {
            logger.warn(`No data found for master_config_id: ${versionId}`);
        }

        res.json({ status: 'SUCCESS', data: rows, columns, versionIds, message: '' });
    }

    saveVersion = async (req: Request, res: Response) => {
        const { versionName, versionIds } = req.body;
        logger.debug(`saveVersion API called with versionName: ${versionName}, versionIds: ${JSON.stringify(versionIds)}`);
        logger.debug(`Version IDs received: ${versionIds[1]},${versionIds[2]},${versionIds[3]},${versionIds[4]},${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);

        // Check if a row with these exact version IDs already exists
        const checkQuery = `
            SELECT id
            FROM cm_conf.t_master_config
            WHERE tele2_coverage = $1
                AND tele2_updated = $2
                AND tele2_voice_updated = $3
                AND tim_sparkle_price_updated = $4
                AND tim_sparkle_roaming_updated = $5
                AND hot_mobile_updated = $6
                AND bics_coverage_bands_updated = $7
                AND bics_coverage_updated = $8
                AND bics_price_updated = $9
        `;

        const { rows: existingRows } = await this.postgresQueryRunner.executeQuery(
            checkQuery,
            [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8], versionIds[9]],
            true
        );

        if (existingRows.length > 0) {
            // Row already exists, return error
            const masterConfigId = existingRows[0].id;
            logger.warn(`Version with these IDs already exists in the system with master_config_id: ${masterConfigId}`);
            res.status(400).json({
                status: 'ERROR',
                message: 'This version already exists in the system. Please use different versions.',
                data: { masterConfigId }
            });
            return;
        }

        // Insert new row and get its ID
        const insertConfigQuery = `
            INSERT INTO cm_conf.t_master_config (
                tele2_coverage,
                tele2_updated,
                tele2_voice_updated,
                tim_sparkle_price_updated,
                tim_sparkle_roaming_updated,
                hot_mobile_updated,
                bics_coverage_bands_updated,
                bics_coverage_updated,
                bics_price_updated,
                version_name,
                version_date
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_DATE)
            RETURNING id, version_name || ' ( ' || version_date || ' )' AS new_version
        `;

        const { rows: insertedRows } = await this.postgresQueryRunner.executeQuery(
            insertConfigQuery,
            [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8], versionIds[9], versionName],
            true
        );

        const masterConfigId = insertedRows[0].id;
        const newVersion = insertedRows[0].new_version;
        logger.debug(`Inserted new master_config with id: ${masterConfigId}, version: ${newVersion}`);

        // Insert data into t_master_data using the master_config_id
        const insertDataQuery =
            'WITH master_list_coverage AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_coverage( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8 ' +
            '   ) ' +
            '), ' +
            'master_list_technologies AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_technologies( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $5, ' +
            '       $7 ' +
            '   ) ' +
            '), ' +
            'master_list_prices AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_prices( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_price_zones AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_price_zones( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_1 AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_profile_1( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_2 AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_profile_2( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_3 AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_profile_3( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_4 AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_profile_4( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_5 AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_profile_5( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_comments AS ( ' +
            '   SELECT * ' +
            '   FROM cm_data.f_master_list_comments( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            ') ' +
            'INSERT INTO cm_data.t_master_data ( ' +
            '   master_config_id, plmno_code, mcc_mnc, region, country, operator_name, country_code, mgt, ' +
            '   sparkle_coverage, hot_coverage, tele2_coverage, bics_coverage, ' +
            '   sparkle_2g, sparkle_3g, sparkle_4g, ' +
            '   hot_2g, hot_3g, hot_4g, ' +
            '   tele2_2g, tele2_3g, tele2_4g, ' +
            '   bics_2g, bics_3g, bics_4g, ' +
            '   eprofile_3_tim, hot_zone, eprofile_2_tele2, eprofile_1_bics, ' +
            '   tim_data_per_mb, tim_sms_mo, tim_voice_mo, tim_voice_mt, ' +
            '   hot_data, hot_sms, hot_moc, hot_mtc, ' +
            '   tele2_data, tele2_sms_mo, tele2_voice_mo, tele2_voice_mt, ' +
            '   bics_data, bics_sms, bics_voice_mo, bics_voice_mt, ' +
            '   imsi_donor_tcp1, profile1_pz, profile1_price, profile1_broadband, ' +
            '   imsi_donor_tcp2, profile2_pz, profile2_price, profile2_broadband, ' +
            '   imsi_donor_tcp3, profile3_pz, profile3_price, profile3_broadband, ' +
            '   imsi_donor_tcp4, profile4_pz, profile4_price, profile4_broadband, ' +
            '   imsi_donor_tcp5, profile5_pz, profile5_price, profile5_broadband, ' +
            '   prr, blocked_countries, ' +
            '   comments_profile_1, comments_profile_2, comments_profile_3, comments_profile_4, comments_profile_5 ' +
            ') ' +
            'SELECT DISTINCT ' +
            `   ${masterConfigId}, t1.plmno_code, t1.mcc_mnc, t1.region, t1.country, t1.operator_name, t1.country_code, t1.mgt, ` +
            '   t1.sparkle_coverage, t1.hot_coverage, t1.tele2_coverage, t1.bics_coverage, ' +
            '   t2.sparkle_2g, t2.sparkle_3g, t2.sparkle_4g, ' +
            '   t2.hot_2g_3g, t2.hot_2g_3g, t2.hot_4g, ' +
            '   t2.tele2_2g, t2.tele2_3g, t2.tele2_4g, ' +
            '   t2.bics_2g, t2.bics_3g, t2.bics_4g, ' +
            '   t3.eprofile_3_tim, t3.hot_zone, t3.eprofile_2_tele2, t3.eprofile_1_bics, ' +
            '   t4.tim_data_per_mb, t4.tim_sms_mo, t4.tim_voice_mo, t4.tim_voice_mt, ' +
            '   t4.hot_data, t4.hot_sms, t4.hot_moc, t4.hot_mtc, ' +
            '   t4.tele2_data, t4.tele2_sms_mo, t4.tele2_voice_mo, t4.tele2_voice_mt, ' +
            '   t4.bics_data, t4.bics_sms, t4.bics_voice_mo, t4.bics_voice_mt, ' +
            '   t5.imsi_donor_tcp1, t5.profile1_pz, t5.profile1_price, t5.profile1_broadband, ' +
            '   t6.imsi_donor_tcp2, t6.profile2_pz, t6.profile2_price, t6.profile2_broadband, ' +
            '   t7.imsi_donor_tcp3, t7.profile3_pz, t7.profile3_price, t7.profile3_broadband, ' +
            '   t8.imsi_donor_tcp4, t8.profile4_pz, t8.profile4_price, t8.profile4_broadband, ' +
            '   t9.imsi_donor_tcp5, t9.profile5_pz, t9.profile5_price, t9.profile5_broadband, ' +
            '   t10.prr, t10.blocked_countries, ' +
            '   t11.comments_profile_1, t11.comments_profile_2, t11.comments_profile_3, t11.comments_profile_4, t11.comments_profile_5 ' +
            'FROM master_list_coverage t1 ' +
            'JOIN master_list_technologies t2 ON t1.id = t2.id ' +
            'JOIN master_list_price_zones t3 ON t1.id = t3.id ' +
            'JOIN master_list_prices t4 ON t1.id = t4.id ' +
            'JOIN master_list_profile_1 t5 ON t1.id = t5.id ' +
            'JOIN master_list_profile_2 t6 ON t1.id = t6.id ' +
            'JOIN master_list_profile_3 t7 ON t1.id = t7.id ' +
            'JOIN master_list_profile_4 t8 ON t1.id = t8.id ' +
            'JOIN master_list_profile_5 t9 ON t1.id = t9.id ' +
            'JOIN cm_data.v_master_list_prr_and_blocked_countries t10 ON t1.id = t10.id ' +
            'JOIN master_list_comments t11 ON t1.id = t11.id';

        const { rows } = await this.postgresQueryRunner.executeQuery(
            insertDataQuery,
            [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]],
            true
        );

        logger.debug(`Data inserted successfully for master_config_id: ${masterConfigId}`);
        res.json({ status: 'SUCCESS', data: { masterConfigId, newVersion }, message: 'Version saved successfully' });
    }

}