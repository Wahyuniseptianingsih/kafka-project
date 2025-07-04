import { Link, useNavigate } from 'react-router-dom'


const LandingPage = () => {
  const navigate = useNavigate()
  return (
    
    
    <div className="pt-28 px-6 max-w-6xl mx-auto flex flex-col md:flex-row items-center justify-between">

      {/* Left Side */}
      <div className="md:w-1/2 space-y-6">
        <h1 className="text-5xl font-bold leading-tight">
          Sebarkan Harapan Lewat Donasi Anda
        </h1>
        <div className="space-x-4">
          <button
            onClick={() => navigate('/login')}
            className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded"
          >
            Login
          </button>
          <button
            onClick={() => navigate('/register')}
            className="bg-gray-200 hover:bg-gray-300 text-gray-800 px-4 py-2 rounded"
          >
            Register
          </button>
        </div>
        <p className="text-lg text-gray-700 max-w-md">
          Bantu mereka yang membutuhkan dengan donasi transparan dan aman.
          Setiap kontribusi berarti untuk masa depan yang lebih baik.
        </p>
        <Link
          to="/donasi"
          className="inline-block bg-orange-500 hover:bg-orange-600 text-white px-6 py-3 rounded-lg transition"
        >
          Mulai Berdonasi
        </Link>
      </div>

      {/* Right Side (Image) */}
      <div className="md:w-1/2 mt-10 md:mt-0">
        <img
          src="..\assets\donate.jpg"
          alt="Donasi"
          className="rounded-lg shadow-lg w-full"
        />
      </div>
    </div>
  )
}

export default LandingPage
