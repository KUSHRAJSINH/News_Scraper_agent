import { useEffect, useState } from 'react';
import axios from 'axios';
import { Newspaper, MessageSquare, Video, Activity } from 'lucide-react';

const Dashboard = () => {
  const [stats, setStats] = useState(null);
  const [news, setNews] = useState([]);
  const [tweets, setTweets] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const statsRes = await axios.get('http://localhost:8000/api/stats');
        setStats(statsRes.data);
        
        const newsRes = await axios.get('http://localhost:8000/api/recent?table=news_articles');
        setNews(newsRes.data);

        const tweetsRes = await axios.get('http://localhost:8000/api/recent?table=tweets');
        setTweets(tweetsRes.data);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  if (loading) return <div style={{ padding: '2rem' }}>Loading dashboard...</div>;

  const totalRecords = stats ? Object.values(stats).reduce((acc, curr) => acc + (curr.total || 0), 0) : 0;
  const analyzedRecords = stats ? Object.values(stats).reduce((acc, curr) => acc + (curr.analyzed || 0), 0) : 0;

  return (
    <div className="animate-fade-in">
      <div className="glass-panel" style={{ marginBottom: '2rem', background: 'linear-gradient(135deg, rgba(30,58,138,0.2) 0%, rgba(15,23,42,0.6) 100%)' }}>
        <h1 style={{ fontSize: '2rem' }}>🏛️ Political Campaign Intelligence</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Real-time monitoring dashboard for Ahmedabad & Sanand political data streams</p>
      </div>

      <div className="metric-grid">
        <div className="glass-panel rgb-border delay-1" style={{ textAlign: 'center' }}>
          <div className="metric-value">{totalRecords.toLocaleString()}</div>
          <div className="metric-label">Total Records</div>
        </div>
        <div className="glass-panel delay-1" style={{ textAlign: 'center' }}>
          <div className="metric-value" style={{ color: '#1d9bf0' }}>{stats?.tweets?.total?.toLocaleString() || 0}</div>
          <div className="metric-label" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px' }}><MessageSquare size={16}/> Tweets</div>
        </div>
        <div className="glass-panel delay-2" style={{ textAlign: 'center' }}>
          <div className="metric-value" style={{ color: 'var(--color-success)' }}>{stats?.news_articles?.total?.toLocaleString() || 0}</div>
          <div className="metric-label" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px' }}><Newspaper size={16}/> News Articles</div>
        </div>
        <div className="glass-panel delay-2" style={{ textAlign: 'center' }}>
          <div className="metric-value" style={{ color: 'var(--color-danger)' }}>{stats?.youtube_videos?.total?.toLocaleString() || 0}</div>
          <div className="metric-label" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px' }}><Video size={16}/> YT Videos</div>
        </div>
        <div className="glass-panel delay-3" style={{ textAlign: 'center' }}>
          <div className="metric-value" style={{ color: 'var(--accent-rgb-3)' }}>{totalRecords ? Math.round((analyzedRecords/totalRecords)*100) : 0}%</div>
          <div className="metric-label" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '6px' }}><Activity size={16}/> Sentiment Done</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', mt: '4px' }}>{analyzedRecords}/{totalRecords} rows</div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem' }}>
        <div>
          <h3 style={{ borderLeft: '4px solid var(--color-success)', paddingLeft: '12px', marginBottom: '1.5rem' }}>📰 Latest News</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {news.map((item, i) => (
              <div key={i} className="glass-panel" style={{ padding: '1rem' }}>
                <p style={{ fontWeight: 600, fontSize: '0.9rem', marginBottom: '8px' }}>{item.text.substring(0, 100)}...</p>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                  <span>📍 {item.region} | 🕐 {item.date.substring(0, 16)}</span>
                  <span className={`badge ${item.sentiment}`}>{item.sentiment}</span>
                </div>
              </div>
            ))}
            {news.length === 0 && <p>No news available.</p>}
          </div>
        </div>

        <div>
          <h3 style={{ borderLeft: '4px solid #1d9bf0', paddingLeft: '12px', marginBottom: '1.5rem' }}>🐦 Latest Tweets</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {tweets.map((item, i) => (
              <div key={i} className="glass-panel" style={{ padding: '1rem' }}>
                <p style={{ fontSize: '0.9rem', marginBottom: '8px' }}>{item.text.substring(0, 120)}...</p>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                  <span>📍 {item.region} | 🕐 {item.date.substring(0, 16)}</span>
                  <span className={`badge ${item.sentiment}`}>{item.sentiment}</span>
                </div>
              </div>
            ))}
            {tweets.length === 0 && <p>No tweets available.</p>}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
