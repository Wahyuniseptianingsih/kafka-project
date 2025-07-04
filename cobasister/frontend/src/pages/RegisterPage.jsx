import { useState } from 'react'
import axios from 'axios'
import { useNavigate } from 'react-router-dom'

const RegisterPage = () => {
  const [form, setForm] = useState({ name: '', email: '', password: '', role: 'donatur' })
  const navigate = useNavigate()

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value })
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    try {
      await axios.post('http://localhost:5000/register', form)
      alert('Registrasi berhasil')
      navigate('/login')
    } catch (err) {
      const message = err?.response?.data?.error || err?.message || 'Terjadi kesalahan'
  alert('Gagal registrasi: ' + message)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <form onSubmit={handleSubmit} className="bg-white p-6 rounded shadow-md space-y-4 w-80">
        <h2 className="text-2xl font-bold text-center">Register</h2>
        <input name="name" type="text" placeholder="Nama" className="w-full p-2 border rounded" onChange={handleChange} required />
        <input name="email" type="email" placeholder="Email" className="w-full p-2 border rounded" onChange={handleChange} required />
        <input name="password" type="password" placeholder="Password" className="w-full p-2 border rounded" onChange={handleChange} required />
        <select name="role" className="w-full p-2 border rounded" onChange={handleChange} required>
          <option value="donatur">Donatur</option>
          <option value="penyelenggara">Penyelenggara</option>
          <option value="admin">Admin</option>
        </select>
        <button type="submit" className="w-full bg-blue-500 text-white p-2 rounded">Daftar</button>
      </form>
    </div>
  )
}

export default RegisterPage