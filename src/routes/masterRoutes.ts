import express from 'express';
import { MasterController } from '../controllers/masterController';
import {AuthMiddleware} from "../middleware/authMiddleware";

const router = express.Router();
const authMiddleware = new AuthMiddleware();
const masterController = new MasterController();

router.get('/getSavedVersions', authMiddleware.authenticate, masterController.getSavedVersions);
router.post('/getSavedVersionById', authMiddleware.authenticate, masterController.getSavedVersionById);
router.post('/saveVersion', authMiddleware.authenticate, masterController.saveVersion);

export default router;