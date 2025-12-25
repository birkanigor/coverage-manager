import express from 'express';
import { UploadController } from '../controllers/uploadController';
import {AuthMiddleware} from "../middleware/authMiddleware";

const router = express.Router();
const authMiddleware = new AuthMiddleware();
const uploadController = new UploadController();

router.post('/getDataLoadersConf', authMiddleware.authenticate, uploadController.getDataLoadersConf);
router.post('/uploadData', authMiddleware.authenticate, uploadController.uploadData);
router.post('/uploadHotMobileExcel', authMiddleware.authenticate, uploadController.uploadHotMobileExcel);
router.post('/updateData', authMiddleware.authenticate, uploadController.updateData);
router.post('/getImsiDonorData', authMiddleware.authenticate, uploadController.getImsiDonorData);

export default router;