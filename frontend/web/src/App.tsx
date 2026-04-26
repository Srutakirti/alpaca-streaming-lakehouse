import { Routes, Route, NavLink } from 'react-router-dom'
import Monitoring from './pages/Monitoring'
import Visualization from './pages/Visualization'
import './App.css'

export default function App() {
  return (
    <div className="app">
      <nav className="nav">
        <span className="nav-brand">Alpaca Pipeline</span>
        <NavLink to="/" end className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
          Monitoring
        </NavLink>
        <NavLink to="/viz" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
          Visualization
        </NavLink>
      </nav>
      <main className="main">
        <Routes>
          <Route path="/" element={<Monitoring />} />
          <Route path="/viz" element={<Visualization />} />
        </Routes>
      </main>
    </div>
  )
}
