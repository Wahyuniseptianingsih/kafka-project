const db = require('../config/db')
const sendDonasiEvent = require('../kafka/producer')

exports.kirimDonasi = async (req, res) => {
  const { user_id, campaign, jumlah } = req.body
  try {
    await db.execute(
      'INSERT INTO donasi (user_id, campaign, jumlah) VALUES (?, ?, ?)',
      [user_id, campaign, jumlah]
    )

    await sendDonasiEvent({ user_id, campaign, jumlah, waktu: new Date() })

    res.status(201).json({ message: 'Donasi berhasil dikirim' })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
}
