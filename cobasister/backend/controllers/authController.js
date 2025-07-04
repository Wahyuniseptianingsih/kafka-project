const db = require('../config/db')
const bcrypt = require('bcrypt')
const jwt = require('jsonwebtoken')

exports.registerUser = async (req, res) => {
  const { name, email, password, role } = req.body
  try {
    const hashed = await bcrypt.hash(password, 10)
    await db.execute(
      'INSERT INTO users (name, email, password, role) VALUES (?, ?, ?, ?)',
      [name, email, hashed, role]
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
