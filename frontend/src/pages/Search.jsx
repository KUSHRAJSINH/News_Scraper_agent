import { useState, useEffect } from 'react';
import axios from 'axios';
import { Search as SearchIcon } from 'lucide-react';

const Search = () => {
  const [query, setQuery] = useState('');
  const [source, setSource] = useState('all');
  const [region, setRegion] = useState('all');
  const [limit, setLimit] = useState(20);
  
  const [results, setResults] = useState([]);
  const [searching, setSearching] = useState(false);
  const [availableRegions, setAvailableRegions] = useState([]);

  useEffect(() => {
    const fetchRegions = async () => {
      try {
        const res = await axios.get('http://localhost:8000/api/stats');
        let allRegions = new Set();
        // Aggregating regions from all tables
        if (res.data) {
           Object.values(res.data).forEach(tableData => {
              if (tableData.regions) {
                 Object.keys(tableData.regions).forEach(r => {
                    if (r && r !== 'null') allRegions.add(r);
                 });
              }
           });
        }
        setAvailableRegions(Array.from(allRegions));
      } catch (err) {
        console.error("Failed to fetch regions", err);
      }
    };
    fetchRegions();
  }, []);

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!query.trim()) return;
    setSearching(true);
    try {
      const res = await axios.get(`http://localhost:8000/api/search?query=${encodeURIComponent(query)}&source=${source}&region=${region}&limit=${limit}`);
      setResults(res.data);
    } catch (err) {
      console.error(err);
    } finally {
      setSearching(false);
    }
  };

  return (
    <div className="animate-fade-in" style={{ paddingBottom: '3rem' }}>
      <div className="page-header">
        <h1>🔍 Search Intelligence</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Search across news, tweets, and YouTube data with advanced filters.</p>
      </div>

      <div className="glass-panel rgb-border" style={{ marginBottom: '2rem' }}>
        <form onSubmit={handleSearch} style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          
          <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
            <div style={{ flex: 3, minWidth: '250px' }}>
              <label style={{ fontSize: '0.85rem', marginBottom: '8px', display: 'block', color: 'var(--text-secondary)' }}>Keyword Query</label>
              <input 
                type="text" 
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="e.g. MLA, infrastructure, water..." 
                style={{ 
                  width: '100%', 
                  padding: '12px 16px', 
                  background: 'rgba(255,255,255,0.05)', 
                  border: '1px solid var(--surface-border)', 
                  borderRadius: '8px',
                  color: 'white',
                  fontSize: '1rem'
                }} 
              />
            </div>

            <div style={{ flex: 1, minWidth: '120px' }}>
              <label style={{ fontSize: '0.85rem', marginBottom: '8px', display: 'block', color: 'var(--text-secondary)' }}>Source</label>
              <select 
                value={source} onChange={(e) => setSource(e.target.value)}
                style={{ width: '100%', padding: '12px', background: 'rgba(0,0,0,0.3)', color: 'white', border: '1px solid var(--surface-border)', borderRadius: '8px' }}
              >
                <option value="all">All Sources</option>
                <option value="news">News</option>
                <option value="twitter">Twitter</option>
                <option value="youtube">YouTube</option>
                <option value="facebook">Facebook</option>
              </select>
            </div>

            <div style={{ flex: 1, minWidth: '120px' }}>
              <label style={{ fontSize: '0.85rem', marginBottom: '8px', display: 'block', color: 'var(--text-secondary)' }}>Region</label>
              <select 
                value={region} onChange={(e) => setRegion(e.target.value)}
                style={{ width: '100%', padding: '12px', background: 'rgba(0,0,0,0.3)', color: 'white', border: '1px solid var(--surface-border)', borderRadius: '8px' }}
              >
                <option value="all">All Regions</option>
                {availableRegions.map(r => (
                   <option key={r} value={r}>{r.toUpperCase()}</option>
                ))}
              </select>
            </div>

            <div style={{ flex: 1, minWidth: '90px' }}>
              <label style={{ fontSize: '0.85rem', marginBottom: '8px', display: 'block', color: 'var(--text-secondary)' }}>Limit</label>
              <select 
                value={limit} onChange={(e) => setLimit(Number(e.target.value))}
                style={{ width: '100%', padding: '12px', background: 'rgba(0,0,0,0.3)', color: 'white', border: '1px solid var(--surface-border)', borderRadius: '8px' }}
              >
                <option value={10}>10</option>
                <option value={20}>20</option>
                <option value={50}>50</option>
                <option value={100}>100</option>
              </select>
            </div>
          </div>

          <button type="submit" className="btn btn-primary" disabled={searching} style={{ alignSelf: 'flex-start', marginTop: '0.5rem' }}>
            <SearchIcon size={18} /> {searching ? 'Searching Database...' : 'Search Engine'}
          </button>
        </form>
      </div>

      {results.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <p style={{ color: 'var(--text-secondary)' }}>Found {results.length} results</p>
          {results.map((r, i) => (
            <div key={i} className="glass-panel" style={{ transition: 'all 0.2s' }}>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center', marginBottom: '8px' }}>
                <span style={{ fontSize: '0.7rem', padding: '2px 8px', background: 'rgba(255,255,255,0.1)', borderRadius: '12px', textTransform: 'uppercase', color: 'var(--accent-rgb-2)' }}>{r.src}</span>
                <span style={{ fontWeight: 'bold' }}>{r.title}</span>
              </div>
              <p style={{ fontSize: '0.9rem', color: 'var(--text-secondary)', marginBottom: '12px', lineHeight: '1.5' }}>{r.text?.substring(0, 300)}...</p>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>
                📍 <span style={{ textTransform: 'capitalize' }}>{r.region || '—'}</span> &nbsp;·&nbsp; 🕐 {r.date?.substring(0, 16)} 
                {r.url && <>&nbsp;·&nbsp; <a href={r.url} target="_blank" rel="noreferrer" style={{ color: '#1d9bf0' }}>View Source Link</a></>}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default Search;
