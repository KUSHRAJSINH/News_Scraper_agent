import { useState } from 'react';
import axios from 'axios';
import { BrainCircuit } from 'lucide-react';

const Sentiment = () => {
  const [text, setText] = useState('');
  const [result, setResult] = useState(null);
  const [analyzing, setAnalyzing] = useState(false);

  const analyze = async () => {
    if (!text.trim()) return;
    setAnalyzing(true);
    try {
      const res = await axios.post('http://localhost:8000/api/analyze-sentiment', { text });
      setResult(res.data.result);
    } catch (err) {
      console.error(err);
    } finally {
      setAnalyzing(false);
    }
  };

  return (
    <div className="animate-fade-in">
      <div className="page-header">
        <h1>🧠 Sentiment Lab</h1>
        <p style={{ color: 'var(--text-secondary)' }}>Test the active LLM provider with custom strings.</p>
      </div>

      <div className="glass-panel" style={{ maxWidth: '800px' }}>
        <h3 style={{ marginBottom: '1rem' }}>✍️ Analyze Custom Text</h3>
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="e.g. 'The BJP government has failed to deliver on promises...'"
          style={{
            width: '100%',
            height: '120px',
            background: 'rgba(255,255,255,0.05)',
            border: '1px solid var(--surface-border)',
            borderRadius: '8px',
            padding: '16px',
            color: 'var(--text-primary)',
            fontSize: '1rem',
            marginBottom: '1rem',
            resize: 'vertical'
          }}
        />
        <button className="btn btn-primary" onClick={analyze} disabled={analyzing}>
          <BrainCircuit size={18} /> {analyzing ? 'Analyzing...' : 'Analyze Sentiment'}
        </button>

        {result && (
          <div style={{ marginTop: '2rem', padding: '1.5rem', border: `2px solid ${result.label === 'positive' ? 'var(--color-success)' : result.label === 'negative' ? 'var(--color-danger)' : 'var(--color-warning)'}`, borderRadius: '12px', background: 'rgba(0,0,0,0.2)', textAlign: 'center' }}>
            <div style={{ fontSize: '2.5rem', marginBottom: '8px' }}>
              {result.label === 'positive' ? '😊' : result.label === 'negative' ? '😠' : '😐'}
            </div>
            <h2 style={{ textTransform: 'uppercase' }}>{result.label}</h2>
            <div style={{ color: 'var(--text-secondary)' }}>Score: {result.score?.toFixed(3)}</div>
            <div style={{ fontSize: '0.8rem', marginTop: '8px', color: 'var(--text-muted)' }}>via {result.provider || 'vader'}</div>
          </div>
        )}
      </div>

      <div className="glass-panel" style={{ maxWidth: '800px', marginTop: '2rem' }}>
        <h3 style={{ marginBottom: '1rem', color: 'var(--color-warning)' }}>🔄 Batch Sentiment run on Database</h3>
        <p style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', marginBottom: '1rem' }}>
          Updates all rows in the database that don't have sentiment labels yet.
        </p>
        <button 
          className="btn" 
          onClick={async () => {
             const btn = document.getElementById('batch-btn');
             btn.disabled = true;
             btn.innerText = 'Running batch analysis...';
             await axios.post('http://localhost:8000/api/run-collector/sentiment');
             btn.innerText = '✅ Batch analysis complete!';
             setTimeout(() => { btn.innerText = 'Run Batch Sentiment Analysis'; btn.disabled = false; }, 3000);
          }}
          id="batch-btn"
          style={{ width: '100%', borderColor: 'var(--color-warning)', color: 'var(--color-warning)' }}
        >
          🚀 Run Batch Sentiment Analysis
        </button>
      </div>
    </div>
  );
};

export default Sentiment;
