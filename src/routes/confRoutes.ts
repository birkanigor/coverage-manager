import express from 'express';
import {ConfController} from "../controllers/confController";
import {AuthMiddleware} from "../middleware/authMiddleware";

const router = express.Router();
const authMiddleware = new AuthMiddleware();
const confController = new ConfController();

router.get('/getTcpList', authMiddleware.authenticate, confController.getTcpList);
router.get('/getPzCutOffPoints', authMiddleware.authenticate, confController.getPzCutOffPoints);

export default router;
