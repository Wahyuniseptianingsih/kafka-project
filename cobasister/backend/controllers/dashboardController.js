const db = require('../config/db')

exports.getDashboardData = async (req, res) => {
  try {
    const [userCount] = await db.execute('SELECT COUNT(*) as total FROM users')
    const [donasiTotal] = await db.execute('SELECT SUM(jumlah) as total FROM donasi')
    res.json({
      totalUsers: userCount[0].total,
      totalDonasi: donasiTotal[0].total || 0
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
}
