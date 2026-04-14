import { useState } from 'react';
import axios from 'axios';
import { PlayCircle, Globe, Newspaper, MessageSquare, Video, CheckCircle2, AlertCircle } from 'lucide-react';

const Collectors = () => {
  const [running, setRunning] = useState(false);
  const [logs, setLogs] = useState('');

  const runCollector = async (name) => {
    setRunning(name);
    setLogs(`Starting ${name} collector...\n`);
    try {
      const res = await axios.post(`http://localhost:8000/api/run-collector/${name}`);
      setLogs((prev) => prev + res.data.log + '\n\n' + (res.data.success ? '✅ Success' : '❌ Failed'));
    } catch (err) {
      setLogs((prev) => prev + '\nError connecting to API.');
    } finally {
      setRunning(false);
    }
  };

  const collectors = [
    { id: 'news', name: 'News Scraper', icon: Newspaper, color: 'var(--color-success)' },
    { id: 'youtube', name: 'YouTube Collector', icon: Video, color: 'var(--color-danger)' },
    { id: 'twitter', name: 'Twitter Collector', icon: MessageSquare, color: '#1d9bf0' },
    { id: 'public', name: 'Public Data', icon: Globe, color: 'var(--accent-rgb-3)' },
  ];

  return (
    <div className="animate-fade-in">
      <div className="page-header">
        <h1>⚙️ Run Data Collectors</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Trigger analytics pipelines directly from the browser.</p>
      </div>

      <div className="glass-panel" style={{ marginBottom: '2rem', textAlign: 'center' }}>
        <h3 style={{ marginBottom: '1rem' }}>🚀 Run All Collectors</h3>
        <button 
          className="btn btn-primary" 
          disabled={running} 
          onClick={() => alert("Run All feature to be mapped sequentially in full version")}
          style={{ padding: '12px 30px', fontSize: '1.1rem' }}
        >
          <PlayCircle /> Run Complete Pipeline
        </button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '1.5rem', marginBottom: '2rem' }}>
        {collectors.map((c) => (
          <div key={c.id} className="glass-panel rgb-border" style={{ textAlign: 'center', transitionDelay: '0.1s' }}>
            <c.icon size={48} color={c.color} style={{ margin: '0 auto 1rem' }} />
            <h4 style={{ marginBottom: '4px' }}>{c.name}</h4>
            <p style={{ fontSize: '0.8rem', color: c.color, marginBottom: '1rem' }}>{c.id}</p>
            <button 
              className="btn" 
              disabled={running}
              onClick={() => runCollector(c.id)}
              style={{ width: '100%' }}
            >
              <PlayCircle size={16} /> Run {c.name.split(' ')[0]}
            </button>
          </div>
        ))}
      </div>

      {logs && (
        <div className="glass-panel" style={{ background: '#0a0a0f', fontFamily: 'monospace', color: '#a3e635', maxHeight: '300px', overflowY: 'auto', whiteSpace: 'pre-wrap', fontSize: '0.85rem' }}>
          {logs}
        </div>
      )}
    </div>
  );
};

export default Collectors;
