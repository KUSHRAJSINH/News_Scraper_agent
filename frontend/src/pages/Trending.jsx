import { useState, useEffect } from 'react';
import axios from 'axios';
import { TrendingUp } from 'lucide-react';

const Trending = () => {
  const [trends, setTrends] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get('http://localhost:8000/api/trending?top_n=20')
      .then(res => setTrends(res.data))
      .catch(err => console.error(err))
      .finally(() => setLoading(false));
  }, []);

  const maxCount = trends.length > 0 ? trends[0].count : 1;

  return (
    <div className="animate-fade-in">
      <div className="page-header">
        <h1>🔥 Trending Topics</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Top mentioned keywords across all regions and sources.</p>
      </div>

      {loading ? <p>Loading trends...</p> : (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
          {trends.map((t, i) => (
            <div key={i} className="glass-panel" style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
              <div style={{ width: '30px', fontWeight: 'bold', color: 'var(--text-muted)' }}>#{i+1}</div>
              <div style={{ flex: 1 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                  <span style={{ fontWeight: 600 }}>{t.word}</span>
                  <span style={{ color: 'var(--accent-rgb-2)', fontSize: '0.85rem' }}>{t.count} mentions</span>
                </div>
                <div style={{ background: 'rgba(255,255,255,0.05)', height: '6px', borderRadius: '4px', overflow: 'hidden' }}>
                  <div style={{ 
                    width: `${(t.count / maxCount) * 100}%`, 
                    height: '100%', 
                    background: 'linear-gradient(90deg, var(--accent-rgb-1), var(--accent-rgb-2))' 
                  }}></div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default Trending;
