const mysql = require('mysql2/promise')

const db = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'seberharapan'
})

module.exports = db