import { Request, Response } from 'express';
import { PostgresQueryRunner } from "../dal"
import logger from "../app.logger";

export class DataController {

    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    private getVersionIdsByMasterConfigId = async (versionId: number): Promise<{ [key: number]: number } | null> => {
        logger.debug(`getVersionIdsByMasterConfigId helper called with versionId: ${versionId}`);

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
            return null;
        }

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

       
        return versionIds;
    }

    getOperatorInfoData = async (req: Request, res: Response) => {
        logger.debug(`getOperatorInfoData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select id , plmno_code , mcc_mnc , region , country , operator_name , country_code , mgt \n' +
            'from cm_data.t_operator_info\n' +
            'order by id', [], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    updateOperatorInfoData = async (req: Request, res: Response) => {
        const { id, plmnoCode, mccMnc, region, country, operatorName, countryCode, mgt } = req.body;
        logger.debug(`updateOperatorInfoData API called . id : ${id}, plmnoCode: ${plmnoCode}, mccMnc: ${mccMnc}, region: ${region}, country: ${country}, operatorName: ${operatorName}, countryCode: ${countryCode}, mgt: ${mgt}`)
        const updateQuery = `update cm_data.t_operator_info
set plmno_code=$1, mcc_mnc=$2, region=$3, country=$4, operator_name=$5, country_code=$6, mgt=$7
where id=$8`
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(updateQuery,
            [plmnoCode, mccMnc, region, country, operatorName, countryCode, mgt, id], true);
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };


    insertOperatorInfoData = async (req: Request, res: Response) => {
        const { plmnoCode, mccMnc, region, country, operatorName, countryCode, mgt } = req.body;
        logger.debug(`insertOperatorInfoData API called. plmnoCode: ${plmnoCode}, mccMnc: ${mccMnc}, region: ${region}, country: ${country}, operatorName: ${operatorName}, countryCode: ${countryCode}, mgt: ${mgt}`);

        const insertQuery = `
        INSERT INTO cm_data.t_operator_info (plmno_code, mcc_mnc, region, country, operator_name, country_code, mgt)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            insertQuery,
            [plmnoCode, mccMnc, region, country, operatorName, countryCode, mgt],
            true
        );

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };


    deleteOperatorInfoData = async (req: Request, res: Response) => {
        const { id } = req.body;
        logger.debug(`deleteOperatorInfoData API called. id: ${id}`);

        const deleteQuery = `DELETE FROM cm_data.t_operator_info WHERE id=$1 RETURNING *`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(deleteQuery, [id], true);

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };



    getNbIotData = async (req: Request, res: Response) => {
        const { versionIds } = req.body;
        logger.debug(`getNbIotData API called with versionIds: ${JSON.stringify(versionIds)}`);
        logger.debug(`Version IDs received: ${versionIds[1]}, ${versionIds[5]}, ${versionIds[7]}`);

        const { rows, columns } = await this.postgresQueryRunner.executeQuery('with \n' +
            'operator_info as\n' +
            '(select distinct plmno_code, operator_name\n' +
            'from cm_data.t_operator_info where coalesce(plmno_code,\'\') != \'\'),\n' +
            't_sparkle_roaming_updated as\n' +
            '(select distinct plmno_code , lower(nbiot_outbound) nbiot_outbound \n' +
            'from cm_data.t_sparkle_roaming_updated where version_id = $1 ),\n' +
            'tele2_coverage as\n' +
            '(select distinct tadig_code, nbiot_out from cm_data.t_tele2_coverage where version_id = $2 ),\n' +
            'bics_coverage_bands_updated as \n' +
            '(select barring_reference_bics , max(fra09_nb_iot_launch) fra09_nb_iot_launch \n' +
            'from cm_data.t_bics_coverage_bands_updated\n' +
            'where version_id = $3\n' +
            'group by barring_reference_bics)\n' +
            'select \n' +
            't1.plmno_code ,  \n' +
            'case when lower(t2.nbiot_outbound) ~* \'x\' then \'TRUE\' else \'FALSE\' end TIM , \n' +
            'case when t3.nbiot_out is null then \'FALSE\' else \'TRUE\' end as "TELE2",\n' +
            'case when t4.fra09_nb_iot_launch is null then \'FALSE\' else \'TRUE\' end as "BICS",\n' +
            't1.operator_name\n' +
            'from operator_info t1 left join t_sparkle_roaming_updated t2 on t1.plmno_code = t2.plmno_code \n' +
            'left join tele2_coverage t3 on t1.plmno_code = t3.tadig_code\n' +
            'left join bics_coverage_bands_updated t4 on t1.plmno_code = t4.barring_reference_bics\n' +
            'order by t1.plmno_code', [versionIds[5], versionIds[1], versionIds[7]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getCatMData = async (req: Request, res: Response) => {
        const { versionIds } = req.body;
        logger.debug(`getCatMData API called with versionIds: ${JSON.stringify(versionIds)}`);
        logger.debug(`Version IDs received: ${versionIds[1]}, ${versionIds[5]}, ${versionIds[7]}`);

        const { rows, columns } = await this.postgresQueryRunner.executeQuery('with \n' +
            'operator_info as\n' +
            '(select distinct plmno_code, operator_name , country\n' +
            'from cm_data.t_operator_info where coalesce(plmno_code,\'\') != \'\'),\n' +
            't_sparkle_roaming_updated as\n' +
            '(select distinct plmno_code , lower(lte_m_outbound) lte_m_outbound \n' +
            'from cm_data.t_sparkle_roaming_updated where version_id = $1 ),\n' +
            'tele2_coverage as\n' +
            '(select distinct tadig_code, lte_m_out from cm_data.t_tele2_coverage where version_id = $2 ),\n' +
            'bics_coverage_bands_updated as \n' +
            '(select barring_reference_bics , max(fra09_lte_m_launch) fra09_lte_m_launch \n' +
            'from cm_data.t_bics_coverage_bands_updated\n' +
            'where version_id = $3\n' +
            'group by barring_reference_bics),\n' +
            'imsi_donors_info as\t \n' +
            '(select  \n' +
            't1.plmno_code ,  \n' +
            't5."general" ,\n' +
            'case when lower(t2.lte_m_outbound) ~* \'x\' then \'TRUE\' else \'FALSE\' end "TIM" ,\n' +
            'case when t3.lte_m_out is null then \'FALSE\' else \'TRUE\' end as "TELE2",\n' +
            'case when t4.fra09_lte_m_launch is null then \'FALSE\' else \'TRUE\' end as "BICS",\n' +
            't1.country,\n' +
            't1.operator_name\n' +
            'from operator_info t1 left join t_sparkle_roaming_updated t2 on t1.plmno_code = t2.plmno_code \n' +
            'left join tele2_coverage t3 on t1.plmno_code = t3.tadig_code\n' +
            'left join bics_coverage_bands_updated t4 on t1.plmno_code = t4.barring_reference_bics\n' +
            'left join cm_temp.t_cat_m_general t5 on t1.plmno_code = t5.plmno)\n' +
            'select \n' +
            'plmno_code,\n' +
            '"general" ,\n' +
            '"TIM",\n' +
            '"TELE2",\n' +
            '"BICS",\n' +
            'country,\n' +
            'operator_name,\n' +
            'case when "general" ~* \'true\' and "TIM" ~*\'false\' then \'TRUE*\' else "TIM" end "TIM_general",\n' +
            'case when "general" ~* \'true\' and "TELE2" ~*\'false\' then \'TRUE*\' else "TELE2" end "TELE2_general",\n' +
            'case when "general" ~* \'true\' and "BICS" ~*\'false\' then \'TRUE*\' else "BICS" end "BICS_general"\n' +
            'from imsi_donors_info\t\n', [versionIds[5], versionIds[1], versionIds[7]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    getBapData = async (req: Request, res: Response) => {
        const { id } = req.body;
        logger.debug(`getBapData API called , id : ${id}`)
        if (![1, 2, 3, 4, 5].includes(Number(id))) {
            res.json({ status: 'FAIL', data: [], message: 'Invalid TCP number' });
        } else {
            const { rows, columns } = await this.postgresQueryRunner.executeQuery(
                'with all_baps as\n' +
                '(select id , country country_name, mcc , active active_imsi_donor , imsi_donor imsi_donor_name , 1 tcp\n' +
                'from cm_data.t_telit_next_carrier_list_bap_tcp1\n' +
                'union\n' +
                'select id , country country_name, mcc , active active_imsi_donor , imsi_donor imsi_donor_name , 2 tcp\n' +
                'from cm_data.t_telit_next_carrier_list_bap_tcp2\n' +
                'union\n' +
                'select id , country country_name, mcc , active active_imsi_donor , imsi_donor imsi_donor_name , 3 tcp\n' +
                'from cm_data.t_telit_next_carrier_list_bap_tcp3\n' +
                'union\n' +
                'select id , country country_name, mcc , active active_imsi_donor , imsi_donor imsi_donor_name , 4 tcp\n' +
                'from cm_data.t_telit_next_carrier_list_bap_tcp4\n' +
                'union\n' +
                'select id , country country_name, mcc , active active_imsi_donor , imsi_donor imsi_donor_name , 5 tcp\n' +
                'from cm_data.t_telit_next_carrier_list_bap_tcp5)\n' +
                'select id, country_name, mcc, active_imsi_donor, imsi_donor_name\n' +
                'from all_baps where tcp = $1\n' +
                'order by country_name', [id], true);
            res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
        }
    }

    get2G3GSunsetData = async (req: Request, res: Response) => {
        logger.debug(`get2G3GSunsetData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select id, plmno_code, country_name, operator_name, sunset_2g, sunset_3g \n' +
            'from cm_data.t_2g_3g_sunset\n' +
            'order by id', [], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    update2G3GSunsetData = async (req: Request, res: Response) => {
        const { id, plmnoCode, countryName, operatorName, sunset2g, sunset3g } = req.body;
        logger.debug(`update2G3GSunsetData API called. id: ${id}, plmnoCode: ${plmnoCode}, countryName: ${countryName}, operatorName: ${operatorName}, sunset2g: ${sunset2g}, sunset3g: ${sunset3g}`);

        const updateQuery = `UPDATE cm_data.t_2g_3g_sunset
SET plmno_code=$1, country_name=$2, operator_name=$3, sunset_2g=$4, sunset_3g=$5
WHERE id=$6`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            updateQuery,
            [plmnoCode, countryName, operatorName, sunset2g, sunset3g, id],
            true
        );

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    insert2G3GSunsetData = async (req: Request, res: Response) => {
        const { plmnoCode, countryName, operatorName, sunset2g, sunset3g } = req.body;
        logger.debug(`insert2G3GSunsetData API called. plmnoCode: ${plmnoCode}, countryName: ${countryName}, operatorName: ${operatorName}, sunset2g: ${sunset2g}, sunset3g: ${sunset3g}`);

        const insertQuery = `
        INSERT INTO cm_data.t_2g_3g_sunset (plmno_code, country_name, operator_name, sunset_2g, sunset_3g)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            insertQuery,
            [plmnoCode, countryName, operatorName, sunset2g, sunset3g],
            true
        );

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    delete2G3GSunsetData = async (req: Request, res: Response) => {
        const { id } = req.body;
        logger.debug(`delete2G3GSunsetData API called. id: ${id}`);

        const deleteQuery = `DELETE FROM cm_data.t_2g_3g_sunset WHERE id=$1 RETURNING *`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(deleteQuery, [id], true);

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    getCountriesRoamingProhibitedData = async (req: Request, res: Response) => {
        logger.debug(`getCountriesRoamingProhibitedData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT id, country_name, price_zone \n' +
            'FROM cm_data.t_countries_roaming_prohibited\n' +
            'order by id', [], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    updateCountriesRoamingProhibitedData = async (req: Request, res: Response) => {
        const { id, countryName, priceZone } = req.body;
        logger.debug(`updateCountriesRoamingProhibitedData API called. id: ${id}, countryName: ${countryName}, priceZone: ${priceZone}`);

        const updateQuery = `UPDATE cm_data.t_countries_roaming_prohibited
SET country_name=$1, price_zone=$2
WHERE id=$3`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            updateQuery,
            [countryName, priceZone, id],
            true
        );

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    insertCountriesRoamingProhibitedData = async (req: Request, res: Response) => {
        const { countryName, priceZone } = req.body;
        logger.debug(`insertCountriesRoamingProhibitedData API called. countryName: ${countryName}, priceZone: ${priceZone}`);

        const insertQuery = `
        INSERT INTO cm_data.t_countries_roaming_prohibited (country_name, price_zone)
        VALUES ($1, $2)
        RETURNING *`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            insertQuery,
            [countryName, priceZone],
            true
        );

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    deleteCountriesRoamingProhibitedData = async (req: Request, res: Response) => {
        const { id } = req.body;
        logger.debug(`deleteCountriesRoamingProhibitedData API called. id: ${id}`);

        const deleteQuery = `DELETE FROM cm_data.t_countries_roaming_prohibited WHERE id=$1 RETURNING *`;

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(deleteQuery, [id], true);

        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    };

    getIotlaunchesAndSteeringData = async (req: Request, res: Response) => {
        logger.debug(`getCountriesRoamingProhibitedData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select id, region, country, "operator", mgt_cc_nc, mcc_mnc, tadig_code, gsm_date_outbound, gprs_date_outbound, umts_date_outbound, camel_date_outbound, lte_date_outbound, "5g_nsa_date_outbound", volte_date_outbound, lte_m_date_outbound, nb_iot_date_outbound, nrtrde_date_outbound, steering, "comment", psm_sup_lte_m, edrx_sup_lte_m, psm_sup_nbiot, edrx_sup_nbiot\n' +
            'from cm_data.t_iot_launches_and_steering \n' +
            'order by id', [], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getMasterListData = async (req: Request, res: Response) => {
        const { versionIds } = req.body;
        logger.debug(`getCatMData API called with versionIds: ${JSON.stringify(versionIds)}`);
        logger.debug(`Version IDs received: ${versionIds[1]},${versionIds[2]},${versionIds[3]},${versionIds[4]},${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'with master_list_coverage as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_coverage( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8 ' +
            '   ) ' +
            '), ' +
            'master_list_technologies as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_technologies( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $5, ' +
            '       $7 ' +
            '   ) ' +
            '), ' +
            'master_list_prices as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_prices( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_price_zones as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_price_zones( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_1 as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_profile_1( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_2 as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_profile_2( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_3 as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_profile_3( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_4 as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_profile_4( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_profile_5 as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_profile_5( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            '), ' +
            'master_list_comments as ( ' +
            '   select * ' +
            '   from cm_data.f_master_list_comments( ' +
            '       $4, ' +
            '       $6, ' +
            '       $2, ' +
            '       $1, ' +
            '       $8, ' +
            '       $3 ' +
            '   ) ' +
            ') ' +
            'select distinct ' +
            't1.plmno_code, t1.mcc_mnc, t1.region, t1.country, t1.operator_name, t1.country_code, t1.mgt, ' +
            't1.sparkle_coverage, t1.hot_coverage, t1.tele2_coverage, t1.bics_coverage, ' +
            't2.sparkle_2g, t2.sparkle_3g, t2.sparkle_4g, ' +
            't2.hot_2g_3g hot_2g, t2.hot_2g_3g hot_3g, t2.hot_4g, ' +
            't2.tele2_2g, t2.tele2_3g, t2.tele2_4g, ' +
            't2.bics_2g, t2.bics_3g, t2.bics_4g, ' +
            't3.eprofile_3_tim, t3.hot_zone, t3.eprofile_2_tele2, t3.eprofile_1_bics, ' +
            't4.tim_data_per_mb, t4.tim_sms_mo, t4.tim_voice_mo, t4.tim_voice_mt, ' +
            't4.hot_data, t4.hot_sms, t4.hot_moc, t4.hot_mtc, ' +
            't4.tele2_data, t4.tele2_sms_mo, t4.tele2_voice_mo, t4.tele2_voice_mt, ' +
            't4.bics_data, t4.bics_sms, t4.bics_voice_mo, t4.bics_voice_mt, ' +
            't5.imsi_donor_tcp1, t5.profile1_pz, t5.profile1_price, t5.profile1_broadband, ' +
            't6.imsi_donor_tcp2, t6.profile2_pz, t6.profile2_price, t6.profile2_broadband, ' +
            't7.imsi_donor_tcp3, t7.profile3_pz, t7.profile3_price, t7.profile3_broadband, ' +
            't8.imsi_donor_tcp4, t8.profile4_pz, t8.profile4_price, t8.profile4_broadband, ' +
            't9.imsi_donor_tcp5, t9.profile5_pz, t9.profile5_price, t9.profile5_broadband, ' +
            't10.prr, t10.blocked_countries, ' +
            't11.comments_profile_1, t11.comments_profile_2, t11.comments_profile_3, t11.comments_profile_4, t11.comments_profile_5 ' +
            'from master_list_coverage t1 ' +
            'join master_list_technologies t2 on t1.id = t2.id ' +
            'join master_list_price_zones t3 on t1.id = t3.id ' +
            'join master_list_prices t4 on t1.id = t4.id ' +
            'join master_list_profile_1 t5 on t1.id = t5.id ' +
            'join master_list_profile_2 t6 on t1.id = t6.id ' +
            'join master_list_profile_3 t7 on t1.id = t7.id ' +
            'join master_list_profile_4 t8 on t1.id = t8.id ' +
            'join master_list_profile_5 t9 on t1.id = t9.id ' +
            'join cm_data.v_master_list_prr_and_blocked_countries t10 on t1.id = t10.id ' +
            'join master_list_comments t11 on t1.id = t11.id',
            [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]],
            true
        );
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListTcp1GlobalData = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListTcp1GlobalData API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT plmno_code, mcc_mnc, region, country, operator_name, price_zone, "2g", "3g", "4g", cat_m, nb_iot, "comments", imsi_donor ' +
            'FROM cm_data.f_price_zone_list_tcp1_global(' +
            '$4, ' +  // p_sparkle_price_updated_version_id
            '$6, ' +  // p_hot_mobile_updated_version_id
            '$2, ' +  // p_tele2_updated_version_id
            '$1, ' +  // p_tele2_coverage_version_id
            '$8, ' + // p_bics_coverage_updated_version_id
            '$5, ' +  // p_sparkle_roaming_updated_version_id
            '$7, ' + // p_bics_coverage_bands_updated_version_id
            '$3' +    // p_tele2_voice_updated_version_id
            ') ORDER BY plmno_code', [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListTcp2GlobalData = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListTcp2GlobalData API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT plmno_code, mcc_mnc, region, country, operator_name, price_zone, "2g", "3g", "4g", cat_m, nb_iot, "comments", imsi_donor ' +
            'FROM cm_data.f_price_zone_list_tcp2_global(' +
            '$4, ' +  // p_sparkle_price_updated_version_id
            '$6, ' +  // p_hot_mobile_updated_version_id
            '$2, ' +  // p_tele2_updated_version_id
            '$1, ' +  // p_tele2_coverage_version_id
            '$8, ' + // p_bics_coverage_updated_version_id
            '$5, ' +  // p_sparkle_roaming_updated_version_id
            '$7, ' + // p_bics_coverage_bands_updated_version_id
            '$3' +    // p_tele2_voice_updated_version_id
            ') ORDER BY plmno_code', [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListTcp3GlobalData = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListTcp3GlobalData API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT plmno_code, mcc_mnc, region, country, operator_name, price_zone, "2g", "3g", "4g", cat_m, nb_iot, "comments", imsi_donor ' +
            'FROM cm_data.f_price_zone_list_tcp3_global(' +
            '$4, ' +  // p_sparkle_price_updated_version_id
            '$6, ' +  // p_hot_mobile_updated_version_id
            '$2, ' +  // p_tele2_updated_version_id
            '$1, ' +  // p_tele2_coverage_version_id
            '$8, ' + // p_bics_coverage_updated_version_id
            '$5, ' +  // p_sparkle_roaming_updated_version_id
            '$7, ' + // p_bics_coverage_bands_updated_version_id
            '$3' +    // p_tele2_voice_updated_version_id
            ') ORDER BY plmno_code', [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListTcp4GlobalData = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListTcp4GlobalData API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT plmno_code, mcc_mnc, region, country, operator_name, price_zone, "2g", "3g", "4g", cat_m, nb_iot, "comments", imsi_donor ' +
            'FROM cm_data.f_price_zone_list_tcp4_global(' +
            '$4, ' +  // p_sparkle_price_updated_version_id
            '$6, ' +  // p_hot_mobile_updated_version_id
            '$2, ' +  // p_tele2_updated_version_id
            '$1, ' +  // p_tele2_coverage_version_id
            '$8, ' + // p_bics_coverage_updated_version_id
            '$5, ' +  // p_sparkle_roaming_updated_version_id
            '$7, ' + // p_bics_coverage_bands_updated_version_id
            '$3' +    // p_tele2_voice_updated_version_id
            ') ORDER BY plmno_code', [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListTcp5GlobalData = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListTcp5GlobalData API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT plmno_code, mcc_mnc, region, country, operator_name, price_zone, "2g", "3g", "4g", cat_m, nb_iot, "comments", imsi_donor ' +
            'FROM cm_data.f_price_zone_list_tcp5_global(' +
            '$4, ' +  // p_sparkle_price_updated_version_id
            '$6, ' +  // p_hot_mobile_updated_version_id
            '$2, ' +  // p_tele2_updated_version_id
            '$1, ' +  // p_tele2_coverage_version_id
            '$8, ' + // p_bics_coverage_updated_version_id
            '$5, ' +  // p_sparkle_roaming_updated_version_id
            '$7, ' + // p_bics_coverage_bands_updated_version_id
            '$3' +    // p_tele2_voice_updated_version_id
            ') ORDER BY plmno_code', [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListEprofile1Data = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListEprofile1Data API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            `SELECT DISTINCT
                t1.plmno_code,
                t1.mcc_mnc,
                t1.region,
                t1.country,
                t1.operator_name,
                t3.eprofile_1_bics,
                t2.bics_2g,
                t2.bics_3g,
                t2.bics_4g,
                t12."BICS" AS cat_m,
                t13."BICS" AS nb_iot,
                CASE
                    WHEN t10.prr = 'TRUE'
                        AND (t10.country ~* 'china' OR t10.country ~* 'australia')
                    THEN 'eUICC SIM is required for Permanent Roaming'
                    ELSE ''
                END AS comments
            FROM  cm_data.f_master_list_coverage(
                $4,   
                $6,  
                $2,   
                $1,  
                $8   
            )  t1
            JOIN cm_data.f_master_list_technologies(
                $4,   
                $6,   
                $2, 
                $1,   
                $8,  
                $5,  
                $7  
            ) t2 ON t1.id = t2.id
            JOIN cm_data.f_master_list_price_zones(
                $4, 
                $6, 
                $2, 
                $1, 
                $8, 
                $3 
            ) t3 ON t1.id = t3.id
            JOIN cm_data.v_master_list_prr_and_blocked_countries t10
                ON t1.id = t10.id
            JOIN cm_data.f_master_list_comments(
                $4, 
                $6, 
                $2, 
                $1, 
                $8, 
                $3 
            ) t11
                ON t1.id = t11.id
            JOIN cm_data.f_cat_m(
                $5, 
                $1, 
                $7 
            ) t12
                ON t1.id = t12.id
            JOIN cm_data.f_nb_iot(
                $5, 
                $1, 
                $7 
            ) t13
                ON t1.id = t13.id
            WHERE t3.eprofile_1_bics < 8`, [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListEprofile2Data = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListEprofile2Data API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            `SELECT DISTINCT
                t1.plmno_code,
                t1.mcc_mnc,
                t1.region,
                t1.country,
                t1.operator_name,
                t3.eprofile_1_bics,
                t2.tele2_2g,
                t2.tele2_3g,
                t2.tele2_4g,
                t12."TELE2" AS cat_m,
                t13."TELE2" AS nb_iot,
                (
                    CASE
                        WHEN t10.prr = 'TRUE'
                            AND (t10.country ~* 'china' OR t10.country ~* 'australia')
                        THEN 'eUICC SIM is required for Permanent Roaming'
                        ELSE ''
                    END
                    || ' ' ||
                    CASE
                        WHEN t14.access_fee_per_imsi_eur_month::numeric < 0.2 THEN 'Access Fees Group A'
                        WHEN t14.access_fee_per_imsi_eur_month::numeric >= 0.2 THEN 'Access Fees Group B'
                        ELSE ''
                    END
                ) AS comments
            FROM  cm_data.f_master_list_coverage(
                $4,   
                $6,   
                $2, 
                $1,  
                $8   
            )  t1
            JOIN cm_data.f_master_list_technologies(
                $4,  
                $6,  
                $2,  
                $1,   
                $8, 
                $5, 
                $7  
            ) t2 ON t1.id = t2.id
            JOIN cm_data.f_master_list_price_zones(
                $4, 
                $6,
                $2, 
                $1, 
                $8, 
                $3 
            ) t3 ON t1.id = t3.id
            JOIN cm_data.v_master_list_prr_and_blocked_countries t10  ON t1.id = t10.id
            JOIN cm_data.f_master_list_comments(
                $4, 
                $6,
                $2, 
                $1,
                $8, 
                $3 
            ) t11    ON t1.id = t11.id
            JOIN cm_data.f_cat_m(
                $5, 
                $1,
                $7 
            ) t12    ON t1.id = t12.id
            JOIN cm_data.f_nb_iot(
                $5, 
                $1, 
                $7 
            ) t13  ON t1.id = t13.id
            LEFT JOIN cm_data.t_tele2_updated t14
                ON t1.plmno_code = t14.tadig and t14.version_id = 94 --p_tele2_updated_version_id
            WHERE t3.eprofile_2_tele2 < 8`, [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListEprofile3Data = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListEprofile3Data API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        logger.debug(`Version IDs for master_config_id ${versionId}: ${versionIds[1]}, ${versionIds[2]}, ${versionIds[3]}, ${versionIds[4]}, ${versionIds[5]}, ${versionIds[6]}, ${versionIds[7]}, ${versionIds[8]}, ${versionIds[9]}`);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            `SELECT DISTINCT
                t1.plmno_code,
                t1.mcc_mnc,
                t1.region,
                t1.country,
                t1.operator_name,
                t3.eprofile_1_bics,
                t2.sparkle_2g,
                t2.sparkle_3g,
                t2.sparkle_4g,
                t12."TIM" AS cat_m,
                t13.tim AS nb_iot,
                CASE
                    WHEN t10.prr = 'TRUE'
                        AND (t10.country ~* 'china' OR t10.country ~* 'australia')
                    THEN 'eUICC SIM is required for Permanent Roaming'
                    ELSE ''
                END AS comments
            FROM  cm_data.f_master_list_coverage(
                $4,  
                $6,  
                $2, 
                $1,  
                $8   
            )  t1
            JOIN cm_data.f_master_list_technologies(
                $4,   
                $6,   
                $2,   
                $1,  
                $8, 
                $5,
                $7   
            ) t2 ON t1.id = t2.id
            JOIN cm_data.f_master_list_price_zones(
                $4, 
                $6,
                $2, 
                $1, 
                $8, 
                $3 
            ) t3 ON t1.id = t3.id
            JOIN cm_data.v_master_list_prr_and_blocked_countries t10  ON t1.id = t10.id
            JOIN cm_data.f_master_list_comments(
                $4,
                $6, 
                $2, 
                $1, 
                $8,
                $3 
            ) t11    ON t1.id = t11.id
            JOIN cm_data.f_cat_m(
                $5, 
                $1, 
                $7 
            ) t12    ON t1.id = t12.id
            JOIN cm_data.f_nb_iot(
                $5, 
                $1, 
                $7 
            ) t13  ON t1.id = t13.id
            WHERE t3.eprofile_3_tim < 8`, [versionIds[1], versionIds[2], versionIds[3], versionIds[4], versionIds[5], versionIds[6], versionIds[7], versionIds[8]], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListEprofile4Data = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListEprofile4Data API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT plmno, mcc_mnc, region, country, "operator", pz, "2g", "3g", "4g", cat_m, nb_iot, "comments"\n' +
            'FROM cm_data.t_eprofile_4', [], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }

    getPriceZoneListEprofile5Data = async (req: Request, res: Response) => {
        const { versionId } = req.body;
        logger.debug(`getPriceZoneListEprofile5Data API called with versionId: ${versionId}`)

        const versionIds = await this.getVersionIdsByMasterConfigId(versionId);
        if (!versionIds) {
            res.status(404).json({ status: 'ERROR', message: 'Version not found', data: null });
            return;
        }

        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'SELECT plmno, mcc_mnc, region, country, "operator", pz, "2g", "3g", "4g", cat_m, nb_iot, "comments"\n' +
            'FROM cm_data.t_eprofile_5', [], true)
        res.json({ status: 'SUCCESS', data: rows, columns, message: '' });
    }
}
