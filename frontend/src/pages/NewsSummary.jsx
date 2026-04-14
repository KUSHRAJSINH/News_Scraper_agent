import { useState, useEffect } from 'react';
import axios from 'axios';
import { FileText } from 'lucide-react';

const NewsSummary = () => {
  const [region, setRegion] = useState('all');
  const [limit, setLimit] = useState(10);
  const [summaryData, setSummaryData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [availableRegions, setAvailableRegions] = useState([]);

  useEffect(() => {
    const fetchRegions = async () => {
      try {
        const res = await axios.get('http://localhost:8000/api/stats');
        // Extract unique regions from news_articles table stats
        if (res.data && res.data.news_articles && res.data.news_articles.regions) {
          const regionsObj = res.data.news_articles.regions;
          // Return the keys (region names)
          const distinctRegions = Object.keys(regionsObj).filter(r => r && r !== 'null');
          setAvailableRegions(distinctRegions);
        }
      } catch (err) {
        console.error("Failed to fetch regions", err);
      }
    };
    fetchRegions();
  }, []);

  const handleSummarize = async () => {
    setLoading(true);
    setSummaryData(null);
    try {
      const res = await axios.get(`http://localhost:8000/api/summarize-news?region=${region}&limit=${limit}`);
      setSummaryData(res.data);
    } catch (err) {
      console.error(err);
      setSummaryData({ success: false, error: 'Network error connecting to API' });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="animate-fade-in">
      <div className="page-header">
        <h1>📰 News Summary <span style={{fontSize: '1rem', color: 'var(--accent-rgb-2)'}}>(LLM-Powered)</span></h1>
        <p style={{ color: 'var(--text-secondary)' }}>Summarize political insights dynamically using AI.</p>
      </div>

      <div className="glass-panel rgb-border" style={{ marginBottom: '2rem', maxWidth: '600px' }}>
        <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
          <div style={{ flex: 1 }}>
            <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem' }}>Region</label>
            <select 
              value={region} onChange={(e) => setRegion(e.target.value)}
              style={{ width: '100%', padding: '10px', background: 'rgba(0,0,0,0.3)', color: 'white', border: '1px solid var(--surface-border)', borderRadius: '6px' }}
            >
              <option value="all">Global / All Regions</option>
              {availableRegions.map(r => (
                <option key={r} value={r}>{r.toUpperCase()}</option>
              ))}
            </select>
          </div>
          <div style={{ flex: 1 }}>
            <label style={{ display: 'block', marginBottom: '8px', fontSize: '0.85rem' }}>Articles to process (Max)</label>
            <input 
              type="number" min="5" max="30" value={limit} onChange={(e) => setLimit(e.target.value)}
              style={{ width: '100%', padding: '10px', background: 'rgba(0,0,0,0.3)', color: 'white', border: '1px solid var(--surface-border)', borderRadius: '6px' }}
            />
          </div>
        </div>
        <button className="btn btn-primary" onClick={handleSummarize} disabled={loading} style={{ width: '100%' }}>
          <FileText size={18} /> {loading ? 'Synthesizing with LLM...' : 'Generate Summary'}
        </button>
      </div>

      {summaryData && (
        <div className="glass-panel fade-in delay-1">
          {!summaryData.success ? (
            <p style={{ color: 'var(--color-danger)' }}>❌ {summaryData.error}</p>
          ) : (
            <div>
              <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '1rem', textTransform: 'uppercase' }}>
                Via {summaryData.provider.toUpperCase()}
              </p>
              
              {summaryData.summary ? (
                <div style={{ lineHeight: '1.8', whiteSpace: 'pre-wrap', color: '#e2e8f0' }}>
                  {summaryData.summary}
                </div>
              ) : (
                <div>
                  <p style={{ color: 'var(--color-warning)', fontSize: '0.85rem', marginBottom: '1rem' }}>
                    💡 No LLM configured. Set LLM_PROVIDER in .env. Showing raw items.
                  </p>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                    {summaryData.raw_articles?.map((item, i) => (
                      <div key={i} style={{ padding: '15px', background: 'rgba(255,255,255,0.02)', borderRadius: '8px', borderLeft: '3px solid var(--color-success)' }}>
                        <div style={{ fontWeight: 600, marginBottom: '6px' }}>{item.title}</div>
                        <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>{item.content.substring(0, 150)}...</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default NewsSummary;
