/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        primary: '#FF4B4B',
        surface: '#1a1a2e',
        surface2: '#16213e',
        border: '#2a2a4a',
      },
    },
  },
  plugins: [],
};
