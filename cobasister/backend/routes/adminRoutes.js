const express = require('express');
const router = express.Router();
const { getDashboardStats, logEvent } = require('../controllers/adminController');

router.get('/dashboard', getDashboardStats);
router.post('/log', logEvent);

module.exports = router;
