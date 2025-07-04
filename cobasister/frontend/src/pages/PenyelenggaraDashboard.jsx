import { useState } from 'react'

const PenyelenggaraDashboard = () => {
  const [campaigns, setCampaigns] = useState([
    {
      title: 'Bantu Korban Banjir',
      target: 10000000,
      terkumpul: 4200000,
      status: 'Berjalan',
    },
    {
      title: 'Donasi Pendidikan',
      target: 5000000,
      terkumpul: 5000000,
      status: 'Selesai',
    },
  ])

  return (
    <div className="p-6 bg-gray-100 min-h-screen">
      <h1 className="text-2xl font-bold mb-4">Dashboard Penyelenggara</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {campaigns.map((c, i) => (
          <div key={i} className="bg-white p-4 rounded shadow">
            <h3 className="text-lg font-semibold">{c.title}</h3>
            <p className="text-sm text-gray-600">Target: Rp{c.target.toLocaleString()}</p>
            <p className="text-sm text-gray-600">Terkumpul: Rp{c.terkumpul.toLocaleString()}</p>
            <span
              className={`inline-block mt-2 px-3 py-1 text-sm rounded-full ${
                c.status === 'Selesai' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'
              }`}
            >
              {c.status}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

export default PenyelenggaraDashboard
