import express from 'express';
import { ScreensController } from '../controllers/screensController';
import {AuthMiddleware} from "../middleware/authMiddleware";

const router = express.Router();
const authMiddleware = new AuthMiddleware();
const screensController = new ScreensController();

router.get('/getScreenConfig', authMiddleware.authenticate, screensController.getScreenConfig)

export default router;