import { type ReactNode } from 'react';
import { ThemeProvider } from './context/ThemeContext';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { GameProvider } from './context/GameContext';
import { AuthProvider, useAuth } from './context/AuthContext';
import Layout from './components/layout/layout';
import Home from './pages/Home';
import Home1 from './pages/Home1';
import AuthLayout from './components/auth/AuthLayout';
import SignIn from './pages/auth/SignIn';
import SignUp from './pages/auth/SignUp';
import { Toaster } from 'sonner';

const ProtectedRoute = ({ children }: { children: ReactNode; }) => {
  const { isAuthenticated } = useAuth();
  if (!isAuthenticated) {
    return <Navigate to="/auth/sign-in" replace />;
  }
  return children;
};

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <GameProvider>
          <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
            <Routes>
              <Route path="/" element={<Layout />}>
                <Route index element={<Home />} />
                <Route path="home1" element={<Home1 />} />
              </Route>
              <Route path="/auth" element={<AuthLayout />}>
                <Route path="sign-in" element={<SignIn />} />
                <Route path="sign-up" element={<SignUp />} />
              </Route>
            </Routes>
            <Toaster />
          </ThemeProvider>
        </GameProvider>
      </AuthProvider>
    </BrowserRouter>
  );
}