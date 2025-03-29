import { Request, Response } from 'express';
import {PostgresQueryRunner} from "../dal"
import logger from "../app.logger";

export class DataController {

    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    getOperatorInfoData = async (req: Request, res: Response) => {
        logger.debug(`getOperatorInfoData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select id , plmno_code , mcc_mnc , region , country , operator_name , country_code , mgt \n' +
            'from cm_data.t_operator_info\n' +
            'order by id',[],true)
        res.json({status: 'SUCCESS', data: rows, columns, message: ''});
    };

    getNbIotData = async (req: Request, res: Response) => {
        logger.debug(`getNbIotData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery('with \n' +
            'operator_info as\n' +
            '(select distinct plmno_code, operator_name\n' +
            'from cm_data.t_operator_info where coalesce(plmno_code,\'\') != \'\'),\n' +
            't_sparkle_roaming_updated as\n' +
            '(select distinct plmno_code , lower(nbiot_outbound) nbiot_outbound \n' +
            'from cm_data.t_sparkle_roaming_updated ),\n' +
            'tele2_coverage as\n' +
            '(select distinct tadig_code, nbiot_out from cm_data.t_tele2_coverage ),\n' +
            'bics_coverage_bands_updated as \n' +
            '(select barring_reference_bics , max(fra09_nb_iot_launch) fra09_nb_iot_launch \n' +
            'from cm_data.t_bics_coverage_bands_updated\n' +
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
            'order by t1.plmno_code',[],true)
        res.json({status: 'SUCCESS', data: rows, columns , message: ''});
    }

    getCatMData = async (req: Request, res: Response) => {
        const { rows, columns } = await this.postgresQueryRunner.executeQuery('with \n' +
            'operator_info as\n' +
            '(select distinct plmno_code, operator_name , country\n' +
            'from cm_data.t_operator_info where coalesce(plmno_code,\'\') != \'\'),\n' +
            't_sparkle_roaming_updated as\n' +
            '(select distinct plmno_code , lower(lte_m_outbound) lte_m_outbound \n' +
            'from cm_data.t_sparkle_roaming_updated ),\n' +
            'tele2_coverage as\n' +
            '(select distinct tadig_code, lte_m_out from cm_data.t_tele2_coverage ),\n' +
            'bics_coverage_bands_updated as \n' +
            '(select barring_reference_bics , max(fra09_lte_m_launch) fra09_lte_m_launch \n' +
            'from cm_data.t_bics_coverage_bands_updated\n' +
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
            'from imsi_donors_info\t\n',[],true)
        res.json({status: 'SUCCESS', data: rows, columns, message: ''});
    };

    getBapData = async (req: Request, res: Response) => {
        const { id } = req.body;
        logger.debug(`getBapData API called , id : ${id}`)
        if(![1,2,3,4,5].includes(Number(id))){
            res.json({status: 'FAIL', data: [], message: 'Invalid TCP number'});
        }else{
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
                'order by country_name',[id],true);
            res.json({status: 'SUCCESS', data: rows, columns, message: ''});
        }
    }

    get2G3GSunsetData = async (req: Request, res: Response) => {
        logger.debug(`get2G3GSunsetData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select id, plmno_code, country_name, operator_name, sunset_2g, sunset_3g \n' +
            'from cm_data.t_2g_3g_sunset\n' +
            'order by id',[],true)
        res.json({status: 'SUCCESS', data: rows, columns, message: ''});
    }

    getCountriesRoamingProhibitedData = async (req: Request, res: Response) => {
        logger.debug(`getCountriesRoamingProhibitedData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select id, plmno_code, country_name, operator_name, sunset_2g, sunset_3g \n' +
            'from cm_data.t_2g_3g_sunset\n' +
            'order by id',[],true)
        res.json({status: 'SUCCESS', data: rows, columns, message: ''});
    }

    getIotlaunchesAndSteeringData = async (req: Request, res: Response) => {
        logger.debug(`getCountriesRoamingProhibitedData API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            'select id, region, country, "operator", mgt_cc_nc, mcc_mnc, tadig_code, gsm_date_outbound, gprs_date_outbound, umts_date_outbound, camel_date_outbound, lte_date_outbound, "5g_nsa_date_outbound", volte_date_outbound, lte_m_date_outbound, nb_iot_date_outbound, nrtrde_date_outbound, steering, "comment", psm_sup_lte_m, edrx_sup_lte_m, psm_sup_nbiot, edrx_sup_nbiot\n' +
            'from cm_data.t_iot_launches_and_steering \n' +
            'order by id',[],true)
        res.json({status: 'SUCCESS', data: rows, columns, message: ''});
    }
}
