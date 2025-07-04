import { useState } from 'react'

const DonasiPage = () => {
  const [jumlah, setJumlah] = useState('')
  const [campaign, setCampaign] = useState('')

  const handleSubmit = (e) => {
    e.preventDefault()
    alert(`Donasi sebesar Rp${jumlah} ke ${campaign} berhasil!`)
    // Bisa diganti dengan axios.post() untuk simpan ke backend
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-2xl mx-auto bg-white shadow p-6 rounded-lg">
        <h2 className="text-2xl font-bold mb-4 text-center">Form Donasi</h2>
        <form onSubmit={handleSubmit} className="space-y-4">
          <select
            value={campaign}
            onChange={(e) => setCampaign(e.target.value)}
            required
            className="w-full border p-2 rounded"
          >
            <option value="">Pilih Campaign</option>
            <option value="Bantu Korban Banjir">Bantu Korban Banjir</option>
            <option value="Donasi Pendidikan">Donasi Pendidikan</option>
            <option value="Peduli Anak Yatim">Peduli Anak Yatim</option>
          </select>

          <input
            type="number"
            placeholder="Jumlah Donasi (Rp)"
            className="w-full border p-2 rounded"
            value={jumlah}
            onChange={(e) => setJumlah(e.target.value)}
            required
          />

          <button type="submit" className="w-full bg-green-600 text-white py-2 rounded hover:bg-green-700">
            Donasi Sekarang
          </button>
        </form>
      </div>
    </div>
  )
}

export default DonasiPage
