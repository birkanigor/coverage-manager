import express from 'express';
import { DataController } from '../controllers/dataController';
import {AuthMiddleware} from "../middleware/authMiddleware";

const router = express.Router();
const authMiddleware = new AuthMiddleware();
const dataController = new DataController();

router.get('/getOperatorInfoData', authMiddleware.authenticate, dataController.getOperatorInfoData);
router.post('/updateOperatorInfoData', authMiddleware.authenticate, dataController.updateOperatorInfoData);
router.post('/insertOperatorInfoData', authMiddleware.authenticate, dataController.insertOperatorInfoData);
router.post('/deleteOperatorInfoData', authMiddleware.authenticate, dataController.deleteOperatorInfoData);

router.post('/getNbIotData', authMiddleware.authenticate, dataController.getNbIotData);
router.post('/getCatMData', authMiddleware.authenticate, dataController.getCatMData);
// router.get('/getBapData', authMiddleware.authenticate, dataController.getBapData);

router.post('/getBapData', authMiddleware.authenticate, dataController.getBapData);

router.get('/get2G3GSunsetData', authMiddleware.authenticate, dataController.get2G3GSunsetData);
router.post('/update2G3GSunsetData', authMiddleware.authenticate, dataController.update2G3GSunsetData);
router.post('/insert2G3GSunsetData', authMiddleware.authenticate, dataController.insert2G3GSunsetData);
router.post('/delete2G3GSunsetData', authMiddleware.authenticate, dataController.delete2G3GSunsetData);

router.get('/getCountriesRoamingProhibitedData',authMiddleware.authenticate, dataController.getCountriesRoamingProhibitedData);
router.post('/updateCountriesRoamingProhibitedData', authMiddleware.authenticate, dataController.updateCountriesRoamingProhibitedData);
router.post('/insertCountriesRoamingProhibitedData', authMiddleware.authenticate, dataController.insertCountriesRoamingProhibitedData);
router.post('/deleteCountriesRoamingProhibitedData', authMiddleware.authenticate, dataController.deleteCountriesRoamingProhibitedData);

router.get('/getIotlaunchesAndSteeringData',authMiddleware.authenticate, dataController.getIotlaunchesAndSteeringData);

router.post('/getMasterListData', authMiddleware.authenticate, dataController.getMasterListData);

router.post('/getPriceZoneListTcp1GlobalData', authMiddleware.authenticate, dataController.getPriceZoneListTcp1GlobalData);
router.post('/getPriceZoneListTcp2GlobalData', authMiddleware.authenticate, dataController.getPriceZoneListTcp2GlobalData);
router.post('/getPriceZoneListTcp3GlobalData', authMiddleware.authenticate, dataController.getPriceZoneListTcp3GlobalData);
router.post('/getPriceZoneListTcp4GlobalData', authMiddleware.authenticate, dataController.getPriceZoneListTcp4GlobalData);
router.post('/getPriceZoneListTcp5GlobalData', authMiddleware.authenticate, dataController.getPriceZoneListTcp5GlobalData);

router.post('/getPriceZoneListEprofile1Data', authMiddleware.authenticate, dataController.getPriceZoneListEprofile1Data);
router.post('/getPriceZoneListEprofile2Data', authMiddleware.authenticate, dataController.getPriceZoneListEprofile2Data);
router.post('/getPriceZoneListEprofile3Data', authMiddleware.authenticate, dataController.getPriceZoneListEprofile3Data);
router.post('/getPriceZoneListEprofile4Data', authMiddleware.authenticate, dataController.getPriceZoneListEprofile4Data);
router.post('/getPriceZoneListEprofile5Data', authMiddleware.authenticate, dataController.getPriceZoneListEprofile5Data);
export default router;
