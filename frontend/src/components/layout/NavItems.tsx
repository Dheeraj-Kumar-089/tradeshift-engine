import { Link, useLocation } from 'react-router-dom';

const navItems = [
    { name: 'Dashboard', path: '/' },
    { name: 'Markets', path: '/markets' },
    { name: 'Portfolio', path: '/portfolio' },
    { name: 'Learn', path: '/learn' },
];

export const NavItems = () => {
    const location = useLocation();

    return (
        <nav className="hidden md:flex items-center gap-6">
            {navItems.map((item) => {
                const isActive = location.pathname === item.path;
                return (
                    <Link
                        key={item.path}
                        to={item.path}
                        className={`text-sm font-medium transition-colors duration-200 ${isActive
                                ? 'text-tv-primary'
                                : 'text-tv-text-secondary hover:text-tv-text-primary'
                            }`}
                    >
                        {item.name}
                    </Link>
                );
            })}
        </nav>
    );
};
