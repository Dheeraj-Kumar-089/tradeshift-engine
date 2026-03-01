import { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { User, LogOut, Settings, UserCircle } from 'lucide-react';
import { useAuth } from '../../context/AuthContext';

export const UserDropdown = () => {
    const { isAuthenticated, user, logout } = useAuth();
    const [isOpen, setIsOpen] = useState(false);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const navigate = useNavigate();

    // Close dropdown when clicking outside
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const handleLogout = () => {
        logout();
        setIsOpen(false);
        navigate('/');
    };

    if (!isAuthenticated) {
        return (
            <div className="flex items-center gap-3">
                <button
                    onClick={() => navigate('/auth/sign-in')}
                    className="text-tv-text-primary hover:text-tv-primary font-medium text-sm transition-colors"
                >
                    Log in
                </button>
                <button
                    onClick={() => navigate('/auth/sign-up')}
                    className="bg-tv-primary hover:bg-tv-primary/90 text-white px-4 py-1.5 rounded-full text-sm font-medium transition-colors"
                >
                    Sign up
                </button>
            </div>
        );
    }

    return (
        <div className="relative" ref={dropdownRef}>
            <button
                onClick={() => setIsOpen(!isOpen)}
                className="flex items-center justify-center w-10 h-10 rounded-full bg-tv-bg-base hover:bg-tv-border/50 text-tv-text-primary transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-tv-primary/50"
            >
                <div className="bg-gradient-to-br from-tv-primary to-blue-600 w-full h-full rounded-full flex items-center justify-center text-white font-bold text-lg">
                    {user?.email?.[0]?.toUpperCase() || <UserCircle size={24} />}
                </div>
            </button>

            {isOpen && (
                <div className="absolute right-0 mt-2 w-56 rounded-md shadow-lg bg-tv-bg-pane ring-1 ring-black ring-opacity-5 focus:outline-none border border-tv-border z-50">
                    <div className="py-2">
                        <div className="px-4 py-3 border-b border-tv-border mb-1">
                            <p className="text-sm font-medium text-tv-text-primary">User</p>
                            <p className="text-xs text-tv-text-secondary truncate">{user?.email || 'user@example.com'}</p>
                        </div>

                        <button
                            onClick={() => { setIsOpen(false); navigate('/profile'); }}
                            className="flex w-full items-center px-4 py-2 text-sm text-tv-text-secondary hover:bg-tv-bg-base hover:text-tv-text-primary transition-colors"
                        >
                            <User size={16} className="mr-2" />
                            Profile
                        </button>

                        <button
                            onClick={() => { setIsOpen(false); navigate('/settings'); }}
                            className="flex w-full items-center px-4 py-2 text-sm text-tv-text-secondary hover:bg-tv-bg-base hover:text-tv-text-primary transition-colors"
                        >
                            <Settings size={16} className="mr-2" />
                            Settings
                        </button>

                        <div className="border-t border-tv-border my-1"></div>

                        <button
                            onClick={handleLogout}
                            className="flex w-full items-center px-4 py-2 text-sm text-red-500 hover:bg-red-500/10 transition-colors"
                        >
                            <LogOut size={16} className="mr-2" />
                            Logout
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
};
