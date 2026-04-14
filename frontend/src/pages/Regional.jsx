import { useState } from 'react';
import axios from 'axios';
import { MapPin, Search } from 'lucide-react';

const Regional = () => {
  const [area, setArea] = useState('');
  const [loading, setLoading] = useState(false);
  const [log, setLog] = useState('');

  const handleScrape = async (e) => {
    e.preventDefault();
    if (!area.trim()) return;
    setLoading(true);
    setLog(`🚀 Initializing intelligence gathering for: ${area}...\n\n`);
    
    try {
      const res = await axios.post('http://localhost:8000/api/dynamic-scrape', { area });
      setLog((prev) => prev + res.data.log + `\n\n✅ Complete for ${area}.`);
    } catch (err) {
      setLog((prev) => prev + `\n\n❌ Error: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="animate-fade-in">
      <div className="page-header">
        <h1>📍 Regional Intelligence</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Enter any city or area name to perform a localized crawl across YouTube, Twitter, and News.</p>
      </div>

      <div className="glass-panel rgb-border" style={{ marginBottom: '2rem', maxWidth: '600px' }}>
        <form onSubmit={handleScrape} style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <div>
            <label style={{ fontSize: '0.9rem', fontWeight: 600, display: 'block', marginBottom: '8px' }}>Target Place/City Name</label>
            <div style={{ display: 'flex', gap: '10px' }}>
              <input 
                type="text" 
                value={area}
                onChange={(e) => setArea(e.target.value)}
                placeholder="e.g. Dholka, Bavla, Rajkot..." 
                style={{ 
                  flex: 1, 
                  padding: '12px', 
                  background: 'rgba(255,255,255,0.05)', 
                  border: '1px solid var(--surface-border)', 
                  borderRadius: '8px',
                  color: 'white'
                }} 
              />
              <button type="submit" className="btn btn-primary" disabled={loading}>
                <Search size={18}/> {loading ? 'Gathering...' : 'Start Scrape'}
              </button>
            </div>
          </div>
        </form>
      </div>

      {log && (
        <div className="glass-panel" style={{ background: '#0a0a0f', fontFamily: 'monospace', color: '#a3e635', maxHeight: '400px', overflowY: 'auto', whiteSpace: 'pre-wrap', fontSize: '0.85rem' }}>
          {log}
        </div>
      )}
    </div>
  );
};

export default Regional;
