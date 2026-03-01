import React, { createContext, useContext, useState, useEffect } from 'react';
import { login as apiLogin, register as apiRegister, logout as apiLogout, checkAuthStatus } from '@/services/AuthService';
import { useNavigate } from 'react-router-dom';

interface UserProfile {
    email: string;
    full_name?: string;
    id?: number;
}

interface AuthContextType {
    user: UserProfile | null;
    login: (data: any) => Promise<void>;
    register: (data: any) => Promise<void>;
    logout: () => void;
    isAuthenticated: boolean;
    isLoading: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider: React.FC<{ children: React.ReactNode; }> = ({ children }) => {
    // HYBRID STRATEGY: Load initial state from LocalStorage for speed
    const [user, setUser] = useState<UserProfile | null>(() => {
        const savedUser = localStorage.getItem('user_profile');
        return savedUser ? JSON.parse(savedUser) : null;
    });
    const [isAuthenticated, setIsAuthenticated] = useState<boolean>(!!user);
    const [isLoading, setIsLoading] = useState<boolean>(true);

    // BACKGROUND CHECK: Verify if the cookie is actually valid
    useEffect(() => {
        const verifySession = async () => {
            try {
                const userData = await checkAuthStatus();
                // If successful, update local state (sync)
                setUser(userData);
                localStorage.setItem('user_profile', JSON.stringify(userData));
                setIsAuthenticated(true);
            } catch (error) {
                console.log("⚠️ Backend session invalid. Clearing local state.");
                // Token expired or invalid -> Clear local state
                localStorage.removeItem('user_profile');
                setUser(null);
                setIsAuthenticated(false);
            } finally {
                setIsLoading(false);
            }
        };

        verifySession();
    }, []);

    const login = async (data: any) => {
        const userData = await apiLogin(data);
        // Save minimal profile to LocalStorage (No sensitive tokens!)
        setUser(userData);
        localStorage.setItem('user_profile', JSON.stringify(userData));
        setIsAuthenticated(true);
    };

    const register = async (data: any) => {
        const userData = await apiRegister(data);
        setUser(userData);
        localStorage.setItem('user_profile', JSON.stringify(userData));
        setIsAuthenticated(true);
    };

    const logout = async () => {
        try {
            await apiLogout();
        } catch (e) {
            console.error("Logout failed", e);
        }
        localStorage.removeItem('user_profile');
        setUser(null);
        setIsAuthenticated(false);
    };

    return (
        <AuthContext.Provider value={{ user, login, register, logout, isAuthenticated, isLoading }}>
            {children}
        </AuthContext.Provider>
    );
};

export const useAuth = () => {
    const context = useContext(AuthContext);
    if (context === undefined) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
};
