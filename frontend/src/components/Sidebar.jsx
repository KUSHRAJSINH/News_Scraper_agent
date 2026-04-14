import { NavLink } from 'react-router-dom';
import { Home, Database, PlayCircle, Search, TrendingUp, BrainCircuit, Globe, MapPin, ScrollText } from 'lucide-react';
import { useEffect, useState } from 'react';
import axios from 'axios';

const Sidebar = () => {
  const [dbStatus, setDbStatus] = useState({ online: false, total: 0 });

  useEffect(() => {
    // Attempt to fetch DB status from API
    axios.get('http://localhost:8000/api/status')
      .then(res => setDbStatus(prev => ({ ...prev, online: res.data.db_exists })))
      .catch(() => setDbStatus(prev => ({ ...prev, online: false })));

    axios.get('http://localhost:8000/api/stats')
      .then(res => {
        let total = 0;
        Object.values(res.data).forEach(s => {
          if (s.total) total += s.total;
        });
        setDbStatus(prev => ({ ...prev, total }));
      })
      .catch(() => {});
  }, []);

  const navItems = [
    { name: 'Dashboard', icon: Home, path: '/dashboard' },
    { name: 'Database', icon: Database, path: '/database' },
    { name: 'Run Collectors', icon: PlayCircle, path: '/collectors' },
    { name: 'Regional Intelligence', icon: MapPin, path: '/regional' },
    { name: 'Search Data', icon: Search, path: '/search' },
    { name: 'Trending Topics', icon: TrendingUp, path: '/trending' },
    { name: 'Sentiment Lab', icon: BrainCircuit, path: '/sentiment' },
    { name: 'News Summary', icon: ScrollText, path: '/news-summary' },
  ];

  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <h2 className="rgb-text" style={{ fontSize: '1.8rem', letterSpacing: '0.05em' }}>Political AI</h2>
        <p style={{ color: 'var(--text-muted)', fontSize: '0.8rem', marginTop: '4px' }}>Ahmedabad / Sanand Region</p>
      </div>

      <div style={{ padding: '0 10px', flex: 1 }}>
        <p style={{ fontSize: '0.7rem', color: 'var(--text-muted)', fontWeight: 600, letterSpacing: '0.1em', marginBottom: '12px' }}>NAVIGATION</p>
        
        {navItems.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}
          >
            <item.icon size={18} />
            {item.name}
          </NavLink>
        ))}
      </div>

      <div className="glass-panel" style={{ padding: '15px', marginTop: 'auto' }}>
        <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontWeight: 600, marginBottom: '8px' }}>SYSTEM STATUS</p>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
          <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: dbStatus.online ? 'var(--color-success)' : 'var(--color-danger)' }}></div>
          <span style={{ fontSize: '0.85rem' }}>DB {dbStatus.online ? 'Online' : 'Offline'}</span>
        </div>
        <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>
          Total rows: <span style={{ color: 'var(--accent-rgb-2)', fontWeight: 600 }}>{dbStatus.total.toLocaleString()}</span>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
