import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import Collectors from './pages/Collectors';
import Database from './pages/Database';
import Search from './pages/Search';
import Sentiment from './pages/Sentiment';
import Trending from './pages/Trending';
import Regional from './pages/Regional';
import NewsSummary from './pages/NewsSummary';
import './index.css';

function App() {
  return (
    <Router>
      <div className="app-container">
        <Sidebar />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Navigate to="/dashboard" replace />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/database" element={<Database />} />
            <Route path="/collectors" element={<Collectors />} />
            <Route path="/search" element={<Search />} />
            <Route path="/trending" element={<Trending />} />
            <Route path="/sentiment" element={<Sentiment />} />
            <Route path="/regional" element={<Regional />} />
            <Route path="/news-summary" element={<NewsSummary />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
