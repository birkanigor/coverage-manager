import express from 'express';
import { DataController } from '../controllers/dataController';
import {AuthMiddleware} from "../middleware/authMiddleware";

const router = express.Router();
const authMiddleware = new AuthMiddleware();
const dataController = new DataController();

router.get('/getData', authMiddleware.authenticate, dataController.getData);

export default router;
