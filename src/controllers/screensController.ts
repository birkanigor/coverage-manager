import { Request, Response } from 'express';
import {PostgresQueryRunner} from "../dal"
import logger from "../app.logger";

export class ScreensController{
    private postgresQueryRunner: PostgresQueryRunner = new PostgresQueryRunner(logger)

    getScreenConfig = async (req: Request, res: Response) => {
        logger.debug(`getScreenConfig API called`)
        const { rows, columns } = await this.postgresQueryRunner.executeQuery(
            `with 
sub_screens_level_2 as
	(select sub_screen_id , 
		json_agg(
			json_build_object(
				'subScreenId', id , 
				'subScreenName', sub_screen_levle_2_name,
				'subScreenConf' , jsonb_strip_nulls(jsonb_build_object( 
										'allowAdd', add_data , 
										'allowEdit', edit_data , 
										'allowDelete', delete_data, 
										'allowUpload', upload_data,
										'skipRows', skip_rows )))) sub_screens_level_2
	from cm_conf.t_cm_system_sub_screens_level_2
	group by sub_screen_id),
all_sub_screens as	
	(select screen_id , 
		jsonb_strip_nulls(jsonb_build_object(
			'subScreenId', id , 
			'subScreenName', sub_screen_name,
			'subScreenstitle', sub_screen_name,
			'subScreenConf' , json_build_object( 
									'allowAdd', add_data , 
									'allowEdit', edit_data , 
									'allowDelete', delete_data, 
									'allowUpload', upload_data ),
			'subScreensLevel2', sub_screens_level_2)) sub_screens
	from cm_conf.t_cm_system_sub_screens t1 left join sub_screens_level_2 t2 on t1.id = t2.sub_screen_id ),
aggregated_sub_screens as 
	( select screen_id, json_agg(sub_screens) sub_screens
	  from all_sub_screens
	  group by screen_id)
select 
	 json_agg(json_build_object('screenId', t1.id ,'screenName', t1.screen_name , 'screenTitle', t1.title, 'subScreens', t2.sub_screens) ) all_screens_config
	 from cm_conf.t_cm_system_screens t1 join aggregated_sub_screens t2 on t1.id = t2.screen_id 
`,[],true)
        res.json({status: 'SUCCESS', data: rows, columns, message: ''});
    }
}