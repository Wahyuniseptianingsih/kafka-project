import { useState } from 'react'
import axios from 'axios'
import { useNavigate } from 'react-router-dom'

const LoginPage = () => {
  const [form, setForm] = useState({ email: '', password: '' })
  const navigate = useNavigate()

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value })
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    try {
      const res = await axios.post('http://localhost:5000/login', form)
      localStorage.setItem('token', res.data.token)
      alert('Login berhasil')
      const role = JSON.parse(atob(res.data.token.split('.')[1])).role
      if (role === 'admin') navigate('/admin')
      else if (role === 'penyelenggara') navigate('/penyelenggara')
      else navigate('/donasi')
    } catch (err) {
      alert('Login gagal: ' + err.response.data.message)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <form onSubmit={handleSubmit} className="bg-white p-6 rounded shadow-md space-y-4 w-80">
        <h2 className="text-2xl font-bold text-center">Login</h2>
        <input name="email" type="email" placeholder="Email" className="w-full p-2 border rounded" onChange={handleChange} required />
        <input name="password" type="password" placeholder="Password" className="w-full p-2 border rounded" onChange={handleChange} required />
        <button type="submit" className="w-full bg-green-500 text-white p-2 rounded">Masuk</button>
      </form>
    </div>
  )
}

export default LoginPage