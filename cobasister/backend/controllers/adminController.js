const pool = require('../models/db');
const { kafkaProducer } = require('../kafka/producer');

exports.getDashboardStats = async (req, res) => {
  try {
    const [[userRes]] = await pool.query("SELECT COUNT(*) AS total FROM users");
    const [[donationRes]] = await pool.query("SELECT COUNT(*) AS total FROM donations");
    const [[campaignRes]] = await pool.query("SELECT COUNT(*) AS total FROM campaigns");
    const [[raisedRes]] = await pool.query("SELECT SUM(amount) AS total FROM donations WHERE status = 'confirmed'");

    const stats = {
      totalUsers: userRes.total,
      totalDonations: donationRes.total,
      totalCampaigns: campaignRes.total,
      totalRaised: raisedRes.total || 0
    };

    await kafkaProducer('admin-dashboard-log', JSON.stringify({
      event_type: 'dashboard_access',
      description: 'Admin accessed dashboard'
    }));

    res.json(stats);
  } catch (err) {
    console.error('Dashboard Error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
};

exports.logEvent = async (req, res) => {
  const { event_type, description } = req.body;
  try {
    await pool.query(
      'INSERT INTO admin_logs (event_type, description) VALUES (?, ?)',
      [event_type, description]
    );
    res.status(200).json({ message: 'Log saved' });
  } catch (err) {
    console.error('Log Error:', err);
    res.status(500).json({ error: 'Log Failed' });
  }
};
