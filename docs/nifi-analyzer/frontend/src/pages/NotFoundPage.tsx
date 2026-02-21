import React from 'react';
import { useNavigate } from 'react-router-dom';

export default function NotFoundPage() {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-950 text-gray-100">
      <div className="text-center">
        <h1 className="text-6xl font-bold text-gray-700 mb-4">404</h1>
        <h2 className="text-xl font-semibold text-gray-300 mb-2">Page not found</h2>
        <p className="text-sm text-gray-500 mb-8">The page you are looking for does not exist or has been moved.</p>
        <div className="flex items-center justify-center gap-3">
          <button
            onClick={() => navigate('/dashboard')}
            className="px-4 py-2 bg-primary hover:bg-primary/90 text-white font-medium rounded-lg text-sm transition"
          >
            Go to Dashboard
          </button>
          <button
            onClick={() => navigate(-1)}
            className="px-4 py-2 bg-gray-800 hover:bg-gray-700 text-gray-300 rounded-lg text-sm transition"
          >
            Go Back
          </button>
        </div>
      </div>
    </div>
  );
}
