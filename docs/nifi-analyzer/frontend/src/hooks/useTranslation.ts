import { useCallback, useState } from 'react';
import en from '../i18n/en.json';

type NestedRecord = { [key: string]: string | NestedRecord };

const translations: Record<string, NestedRecord> = {
  en,
};

const LOCALE_STORAGE_KEY = 'app_locale';

function getInitialLocale(): string {
  try {
    const stored = localStorage.getItem(LOCALE_STORAGE_KEY);
    if (stored && translations[stored]) return stored;
  } catch (err) {
    console.warn('Translation: failed to read locale from localStorage', err);
  }
  return 'en';
}

function getNestedValue(obj: NestedRecord, path: string): string {
  const keys = path.split('.');
  let current: string | NestedRecord = obj;
  for (const key of keys) {
    if (typeof current !== 'object' || current === null) return path;
    current = (current as NestedRecord)[key];
  }
  return typeof current === 'string' ? current : path;
}

export function useTranslation() {
  const [locale, setLocaleState] = useState(getInitialLocale);

  const setLocale = useCallback((newLocale: string) => {
    setLocaleState(newLocale);
    try {
      localStorage.setItem(LOCALE_STORAGE_KEY, newLocale);
    } catch (err) {
      console.warn('Translation: failed to save locale to localStorage', err);
    }
  }, []);

  const t = useCallback(
    (key: string, fallback?: string): string => {
      const dict = translations[locale] || translations.en;
      const value = getNestedValue(dict, key);
      return value !== key ? value : fallback || key;
    },
    [locale],
  );

  return { t, locale, setLocale };
}
