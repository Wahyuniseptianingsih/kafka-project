import { FaUser, FaChartBar, FaPeopleArrows, FaUserShield } from 'react-icons/fa'
import "../index.css"

const AdminDashboard = () => {
  return (
    <div className="flex h-screen bg-gray-100">
      {/* SIDEBAR */}
      <aside className="w-64 bg-white shadow-md p-4">
        <h1 className="text-xl font-bold text-blue-600 mb-6">💧 SebarHarapan</h1>
        <nav className="space-y-4">
          <a href="#" className="flex items-center text-gray-700 hover:text-blue-500">
            <FaUser className="mr-2" /> User Management 
          </a>
          <a href="#" className="flex items-center text-gray-700 hover:text-blue-500">
            <FaUserShield className="mr-2" /> Organizer Management
          </a>
          <a href="#" className="flex items-center text-gray-700 hover:text-blue-500">
            <FaChartBar className="mr-2" /> Report Management
          </a>
        </nav>
      </aside>

      {/* MAIN CONTENT */}
      <main className="flex-1 p-6">
        {/* HEADER */}
        <header className="flex justify-between items-center mb-6">
          <div>
            <h2 className="text-2xl font-semibold">Dashboard Overview</h2>
            <p className="text-gray-500">Sistem Monitoring Donasi SebarHarapan</p>
          </div>
          <div className="bg-white px-4 py-2 rounded shadow text-sm">
            <strong>Admin</strong>
          </div>
        </header>

        {/* STATISTIC CARDS */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          <Card title="Total Users" value="277" color="blue" icon={<FaUser />} />
          <Card title="Total Donations" value="338" color="green" icon={<FaChartBar />} />
          <Card title="Total Campaigns" value="557" color="purple" icon={<FaUserShield />} />
          <Card title="Total Raised" value="$3,787,681" color="orange" icon={<FaPeopleArrows />} />
        </div>
      </main>
    </div>
  )
}

const Card = ({ title, value, color, icon }) => {
  const bgColor = {
    blue: 'bg-blue-500',
    green: 'bg-green-500',
    purple: 'bg-purple-500',
    orange: 'bg-orange-500',
  }

  return (
    <div className={`${bgColor[color]} p-5 rounded-lg text-white shadow`}>
      <div className="flex justify-between items-center">
        <div>
          <p className="text-sm">{title}</p>
          <h3 className="text-2xl font-bold">{value}</h3>
        </div>
        <div className="text-3xl">{icon}</div>
      </div>
    </div>
  )
}

export default AdminDashboard
