import { create } from 'zustand';
import { persist } from 'zustand/middleware';

type Theme = 'light' | 'dark';

interface ThemeState {
    theme: Theme;
    toggleTheme: () => void;
    setTheme: (theme: Theme) => void;
}

export const useThemeStore = create<ThemeState>()(
    persist(
        (set) => ({
            theme: 'dark', // Default to dark execution
            toggleTheme: () =>
                set((state) => {
                    const newTheme = state.theme === 'light' ? 'dark' : 'light';
                    updateDomClass(newTheme);
                    return { theme: newTheme };
                }),
            setTheme: (theme) => {
                updateDomClass(theme);
                set({ theme });
            },
        }),
        {
            name: 'trade-sim-theme',
            onRehydrateStorage: () => (state) => {
                if (state) {
                    updateDomClass(state.theme);
                }
            }
        }
    )
);

// Helper to update the HTML class
const updateDomClass = (theme: Theme) => {
    const root = window.document.documentElement;
    root.classList.remove('light', 'dark');
    root.classList.add(theme);
    root.style.colorScheme = theme;
};
