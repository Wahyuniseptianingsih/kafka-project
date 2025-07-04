const DashboardCard = ({ title, value, icon, color }) => {
  return (
    <div className={`rounded-xl p-4 text-white shadow-md ${color}`}>
      <div className="text-sm font-medium">{title}</div>
      <div className="text-2xl font-bold mt-2">{value}</div>
      <div className="text-right mt-3 text-xl">{icon}</div>
    </div>
  );
};

export default DashboardCard;
