import { useState, useEffect } from 'react';
import axios from 'axios';

const Database = () => {
  const [stats, setStats] = useState(null);
  const [tableData, setTableData] = useState({});

  useEffect(() => {
    const fetchDB = async () => {
      try {
        const res = await axios.get('http://localhost:8000/api/stats');
        setStats(res.data);
        
        // Fetch recent rows for each table
        const tables = Object.keys(res.data);
        const dataMap = {};
        for (const table of tables) {
          if (res.data[table].total > 0) {
            const dataRes = await axios.get(`http://localhost:8000/api/recent?table=${table}&limit=8`);
            dataMap[table] = dataRes.data;
          }
        }
        setTableData(dataMap);
      } catch (err) {
        console.error("Error fetching DB stats", err);
      }
    };
    fetchDB();
  }, []);

  return (
    <div className="animate-fade-in" style={{ paddingBottom: '3rem' }}>
      <div className="page-header">
        <h1>🗄️ Database Management</h1>
        <p style={{ color: 'var(--text-secondary)' }}>View comprehensive stats and recent records for all collected data tables.</p>
      </div>

      {!stats ? <p>Loading database information...</p> : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          {Object.entries(stats).map(([table, data]) => (
            <div key={table} className="glass-panel rgb-border" style={{ overflow: 'hidden' }}>
              <div style={{ padding: '0 0.5rem' }}>
                <h3 style={{ textTransform: 'uppercase', color: 'var(--text-primary)' }}>{table.replace('_', ' ')}</h3>
                
                <div style={{ display: 'flex', gap: '2.5rem', marginTop: '1rem', marginBottom: '1.5rem', borderTop: '1px solid var(--surface-border)', paddingTop: '1.5rem' }}>
                  <div>
                    <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Total Records</div>
                    <div style={{ fontSize: '1.8rem', fontWeight: '800', color: 'var(--accent-rgb-2)' }}>{data.total?.toLocaleString() || 0}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Sentiment Analyzed</div>
                    <div style={{ fontSize: '1.8rem', fontWeight: '800', color: 'var(--color-success)' }}>{data.analyzed?.toLocaleString() || 0}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Latest Entry</div>
                    <div style={{ fontSize: '1.1rem', fontWeight: '500', color: '#e2e8f0', marginTop: '0.4rem' }}>{data.latest ? data.latest.substring(0, 19) : '—'}</div>
                  </div>
                </div>
              </div>

              {/* Data Table */}
              {tableData[table] && tableData[table].length > 0 && (
                <div style={{ marginTop: '1rem', overflowX: 'auto', background: 'rgba(0,0,0,0.3)', borderRadius: '8px', border: '1px solid var(--surface-border)' }}>
                   <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--surface-border)', fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-secondary)' }}>
                     RECENT RECORDS (Limit 8)
                   </div>
                   <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem', textAlign: 'left' }}>
                     <thead>
                       <tr style={{ background: 'rgba(255,255,255,0.02)' }}>
                         <th style={{ padding: '12px 16px', color: 'var(--text-muted)', fontWeight: 500 }}>Text / Title</th>
                         <th style={{ padding: '12px 16px', color: 'var(--text-muted)', fontWeight: 500 }}>Date</th>
                         <th style={{ padding: '12px 16px', color: 'var(--text-muted)', fontWeight: 500 }}>Region</th>
                         <th style={{ padding: '12px 16px', color: 'var(--text-muted)', fontWeight: 500 }}>Sentiment</th>
                       </tr>
                     </thead>
                     <tbody>
                       {tableData[table].map((row, idx) => (
                         <tr key={idx} style={{ borderTop: '1px solid var(--surface-border)', transition: 'background 0.2s', ':hover': {background: 'rgba(255,255,255,0.05)'} }}>
                           <td style={{ padding: '12px 16px', color: '#e2e8f0', maxWidth: '400px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                             {row.text}
                           </td>
                           <td style={{ padding: '12px 16px', color: 'var(--text-secondary)' }}>{row.date ? String(row.date).substring(0,16) : '—'}</td>
                           <td style={{ padding: '12px 16px', color: 'var(--text-secondary)' }}>
                             <span style={{ background: 'rgba(255,255,255,0.1)', padding: '2px 8px', borderRadius: '12px', fontSize: '0.75rem', textTransform: 'capitalize' }}>
                               {row.region || '—'}
                             </span>
                           </td>
                           <td style={{ padding: '12px 16px' }}>
                             <span className={`badge ${row.sentiment?.toLowerCase() || 'neutral'}`}>{row.sentiment || '—'}</span>
                           </td>
                         </tr>
                       ))}
                     </tbody>
                   </table>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default Database;
