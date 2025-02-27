import express from 'express';
import { getData } from '../controllers/dataController';
import {AuthMiddleware} from "../middleware/authMiddleware";

const router = express.Router();
const authMiddleware = new AuthMiddleware();

router.get('/getData', authMiddleware.authenticate, getData);

export default router;
