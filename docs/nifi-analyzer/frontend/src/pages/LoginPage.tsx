import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import LoginForm from '../components/auth/LoginPage';
import RegisterForm from '../components/auth/RegisterPage';
import { useAuthStore } from '../store/auth';

export default function LoginPage() {
  const [mode, setMode] = useState<'login' | 'register'>('login');
  const { token } = useAuthStore();
  const navigate = useNavigate();
  const location = useLocation();

  // If already logged in, redirect
  useEffect(() => {
    if (token) {
      const from = (location.state as { from?: { pathname: string } })?.from?.pathname || '/dashboard';
      navigate(from, { replace: true });
    }
  }, [token, navigate, location]);

  if (mode === 'register') {
    return <RegisterForm onSwitchToLogin={() => setMode('login')} />;
  }

  return <LoginForm onSwitchToRegister={() => setMode('register')} />;
}
