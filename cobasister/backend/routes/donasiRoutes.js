const express = require('express')
const { kirimDonasi } = require('../controllers/donasiController')
const router = express.Router()

router.post('/kirim', kirimDonasi)

module.exports = router
