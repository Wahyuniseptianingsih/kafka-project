// File: backend/routes/authRoutes.js
const express = require('express')
const router = express.Router()
const { registerUser, loginUser } = require('../controllers/authController')

router.post('/register', registerUser)
router.post('/login', loginUser)

module.exports = router

// File: backend/controllers/authController.js
const db = require('../config/db')
const bcrypt = require('bcrypt')
const jwt = require('jsonwebtoken')

exports.registerUser = async (req, res) => {
  const { nama, email, password, role } = req.body
  try {
    const hashedPassword = await bcrypt.hash(password, 10)
    await db.execute(
      'INSERT INTO users (nama, email, password, role) VALUES (?, ?, ?, ?)',
      [nama, email, hashedPassword, role]
    )
    res.status(201).json({ message: 'Registrasi berhasil' })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
}

exports.loginUser = async (req, res) => {
  const { email, password } = req.body
  try {
    const [rows] = await db.execute('SELECT * FROM users WHERE email = ?', [email])
    if (rows.length === 0) return res.status(401).json({ message: 'Email tidak ditemukan' })

    const user = rows[0]
    const isMatch = await bcrypt.compare(password, user.password)
    if (!isMatch) return res.status(401).json({ message: 'Password salah' })

    const token = jwt.sign({ id: user.id, role: user.role }, 'secret-key', { expiresIn: '1d' })
    res.json({ message: 'Login berhasil', token })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
}

// File: backend/config/db.js (unchanged)
const mysql = require('mysql2/promise')
// const db = mysql.createPool({
//   host: 'localhost',
//   user: 'root',
//   password: '',
//   database: 'seberharapan'
// })
// module.exports = db

