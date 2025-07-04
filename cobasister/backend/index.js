const express = require('express')
const cors = require('cors')
const app = express()

app.use(cors())
app.use(express.json())

app.use(require('./routes/authRoutes'))
app.use('/dashboard', require('./routes/dashboardRoutes'))
app.use('/donasi', require('./routes/donasiRoutes'))

app.listen(5000, () => console.log('Server running on http://localhost:5000'))
