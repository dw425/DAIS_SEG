import React, { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import RegisterForm from '../components/auth/RegisterPage';
import { useAuthStore } from '../store/auth';

export default function RegisterPage() {
  const { token } = useAuthStore();
  const navigate = useNavigate();

  useEffect(() => {
    if (token) {
      navigate('/dashboard', { replace: true });
    }
  }, [token, navigate]);

  return <RegisterForm onSwitchToLogin={() => navigate('/login')} />;
}
